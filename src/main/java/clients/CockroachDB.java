package clients;

import clients.utils.TransactionStatistics;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;


public class CockroachDB {

    // Limit number of txns executed
    private static final int TXN_LIMIT = 200;

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("run the program by: ./CockroachDB <host> <port> <schema_name> <client>\n e.g. ./CockroachDB localhost 26267 A 1");
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String schema = args[2];
        String client = args[3];

        String schema_name = "schema_a";
        String dataDir = "project_files/xact_files_A/1.txt";

        if (Objects.equals(schema, "A")) {
            schema_name = "schema_a";
            dataDir = "project_files/xact_files_A/" + client + ".txt";
        } else if (Objects.equals(schema, "B")) {
            schema_name = "schema_a";
            dataDir = "project_files/xact_files_B/" + client + ".txt";
        } else {
            System.err.println("run the program by: ./CockroachDB <host> <port> <schema_name> <client>\n e.g. ./CockroachDB localhost 26267 A 1");
            return;
        }

        System.out.printf("Running on host: %s:%d", host, port);
        System.out.println();

        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName(host);
        ds.setPortNumber(port);
        ds.setDatabaseName(schema_name);
        ds.setUser("root");
        ds.setPassword(null);
//        ds.setSsl(true);
//        ds.setSslMode("require");
//        ds.setSslCert("certs/client.root.crt");
//        ds.setSslKey("certs/client.root.key.pk8");
        ds.setSslMode("disable");
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("CockroachDB App");

        Connection conn = ds.getConnection();
        conn.setAutoCommit(false);
        FileInputStream stream = new FileInputStream(dataDir);
        Scanner scanner = new Scanner(stream);

        System.out.println("Ready to read Xact file " + dataDir);

        ArrayList<TransactionStatistics> latencies = new ArrayList<>();
        int txnCount = 0;
        while (scanner.hasNextLine() && txnCount < TXN_LIMIT) {
            txnCount++;
            String line = scanner.nextLine();
            String[] splits = line.split(",");
            char txnType = splits[0].toCharArray()[0];
            long latency = invokeTransaction(conn, splits, scanner);
            latencies.add(new TransactionStatistics(txnType, latency));
            System.out.printf("<%d/20000> Tnx %c: %dms \n", txnCount, txnType, latency);
        }
        TransactionStatistics.printStatistics(latencies);
    }

    private static long invokeTransaction(Connection conn, String[] splits, Scanner scanner) {
        long start = System.currentTimeMillis();
        switch (splits[0].toCharArray()[0]) {
            case 'N':
                int cid = Integer.parseInt(splits[1]);
                int wid = Integer.parseInt(splits[2]);
                int did = Integer.parseInt(splits[3]);
                int numItems = Integer.parseInt(splits[4]);
                ArrayList<Integer> items = new ArrayList<Integer>();
                ArrayList<Integer> supplierWarehouses = new ArrayList<Integer>();
                ArrayList<Integer> quantities = new ArrayList<Integer>();
                for (int i = 0; i < numItems; i++) {
                    String line = scanner.nextLine();
                    splits = line.split(",");
                    items.add(Integer.parseInt(splits[0]));
                    supplierWarehouses.add(Integer.parseInt(splits[1]));
                    quantities.add(Integer.parseInt(splits[2]));
                }
                newOrderTransaction(conn, cid, wid, did, numItems, items, supplierWarehouses, quantities);
                break;

            case 'P':
                int cwid = Integer.parseInt(splits[1]);
                int cdid = Integer.parseInt(splits[2]);
                cid = Integer.parseInt(splits[3]);
                BigDecimal payment = new BigDecimal(splits[4]);
                paymentTransaction(conn, cwid, cdid, cid, payment);
                break;

            case 'D':
                wid = Integer.parseInt(splits[1]);
                int carrierid = Integer.parseInt(splits[2]);
                deliveryTransaction(conn, wid, carrierid);
                break;

            case 'O':
                cwid = Integer.parseInt(splits[1]);
                cdid = Integer.parseInt(splits[2]);
                cid = Integer.parseInt(splits[3]);
                orderStatusTransaction(conn, cwid, cdid, cid);
                break;

            case 'S':
                wid = Integer.parseInt(splits[1]);
                did = Integer.parseInt(splits[2]);
                int t = Integer.parseInt(splits[3]);
                int l = Integer.parseInt(splits[4]);
                stockLevelTransaction(conn, wid, did, t, l);
                break;

            case 'I':
                wid = Integer.parseInt(splits[1]);
                did = Integer.parseInt(splits[2]);
                l = Integer.parseInt(splits[3]);
                popularItemTransaction(conn, wid, did, l);
                break;

            case 'T':
                topBalanceTransaction(conn);
                break;

            case 'R':
                cwid = Integer.parseInt(splits[1]);
                cdid = Integer.parseInt(splits[2]);
                cid = Integer.parseInt(splits[3]);
                relatedCustomerTransaction(conn, cwid, cdid, cid);
                break;
        }

        return System.currentTimeMillis() - start;
    }

    private static void newOrderTransaction(Connection conn, int cid, int wid, int did, int number_of_items,
                                            ArrayList<Integer> items, ArrayList<Integer> supplier_warehouses, ArrayList<Integer> quantities) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM district_tab WHERE D_W_ID = " + wid + " AND D_ID = " + did);
            Integer next_order_id = 0;
            while (rs.next()) {
                next_order_id = rs.getInt("D_NEXT_O_ID");
                break;
            }

            next_order_id += 1;

            PreparedStatement updateDistrict = conn.prepareStatement(
                    "UPDATE district_tab SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?");
            updateDistrict.setInt(1, next_order_id);
            updateDistrict.setInt(2, wid);
            updateDistrict.setInt(3, did);
            updateDistrict.executeUpdate();

            int all_local = 0;
            if (supplier_warehouses.stream().distinct().count() <= 1 && supplier_warehouses.contains(wid)) {
                all_local = 1;
            }

            PreparedStatement createOrder = conn.prepareStatement("INSERT INTO order_tab (O_W_ID, O_D_ID, O_ID, O_C_ID," +
                    " O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (?,?,?,?,?,?,?,?)");
            createOrder.setInt(1, wid);
            createOrder.setInt(2, did);
            createOrder.setInt(3, next_order_id);
            createOrder.setInt(4, cid);
            createOrder.setInt(5, 0);
            createOrder.setInt(6, number_of_items);
            createOrder.setInt(7, all_local);
            Date date = new Date(System.currentTimeMillis());
            createOrder.setDate(8, date);
            createOrder.execute();

            double total_amount = 0;
            ArrayList<String> itemNames = new ArrayList<String>();
            ArrayList<Float> itemPrices = new ArrayList<Float>();
            ArrayList<Integer> itemStocks = new ArrayList<Integer>();

            for (int idx = 0; idx < items.size(); idx++) {
                int current_item = items.get(idx);

                rs = stmt.executeQuery("SELECT i_price, i_name FROM item_tab WHERE i_id = " + current_item);
                float price = 0;
                String name = "";
                while (rs.next()) {
                    price = rs.getFloat("i_price");
                    name = rs.getString("i_name");
                    break;
                }

                itemNames.add(name);
                itemPrices.add(price);

                String strDid = "";
                if (did == 10) {
                    strDid = String.valueOf(did);
                } else {
                    strDid = "0" + did;
                }

                PreparedStatement itemStock = conn.prepareStatement("SELECT s_quantity,s_ytd,s_order_cnt," +
                        "s_remote_cnt, s_dist_" + strDid + " FROM stock_tab WHERE s_w_id=? AND s_i_id=?;");

                itemStock.setInt(1, wid);
                itemStock.setInt(2, current_item);
                rs = itemStock.executeQuery();

                int s_quantity = 0;
                float s_ytd = 0;
                int s_order_cnt = 0;
                int s_remote_cnt = 0;

                while (rs.next()) {
                    s_quantity = rs.getInt("s_quantity");
                    s_ytd = rs.getFloat("s_ytd");
                    s_order_cnt = rs.getInt("s_order_cnt");
                    s_remote_cnt = rs.getInt("s_remote_cnt");
                    break;
                }
                int adjusted_quantity = s_quantity - quantities.get(idx);
                if (adjusted_quantity < 10) {
                    adjusted_quantity += 100;
                }
                if (supplier_warehouses.get(idx) != wid) {
                    s_remote_cnt += 1;
                }

                PreparedStatement updateStock = conn.prepareStatement("UPDATE stock_tab SET " +
                        "s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE s_w_id=? AND s_i_id=?;");
                updateStock.setInt(1, adjusted_quantity);
                updateStock.setFloat(2, s_ytd + quantities.get(idx));
                updateStock.setInt(3, s_order_cnt + 1);
                updateStock.setInt(4, s_remote_cnt);
                updateStock.setInt(5, wid);
                updateStock.setInt(6, current_item);
                updateStock.executeUpdate();

                itemStocks.add(adjusted_quantity);

                float item_amount = price * quantities.get(idx);
                total_amount += item_amount;

                PreparedStatement insertOrderLine = conn.prepareStatement("INSERT INTO order_line_tab (OL_W_ID, OL_D_ID, " +
                        "OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?);");
                insertOrderLine.setInt(1, wid);
                insertOrderLine.setInt(2, did);
                insertOrderLine.setInt(3, next_order_id);
                insertOrderLine.setInt(4, idx);
                insertOrderLine.setInt(5, current_item);
                insertOrderLine.setTimestamp(6, null);
                insertOrderLine.setFloat(7, item_amount);
                insertOrderLine.setInt(8, supplier_warehouses.get(idx));
                insertOrderLine.setFloat(9, quantities.get(idx));
                insertOrderLine.setString(10, "S_DIST_" + did);
                insertOrderLine.execute();
            }

            double discount = 0;
            String last_name = "";
            String credit = "";
            rs = stmt.executeQuery("SELECT c_last, c_credit, c_discount FROM customer_tab " +
                    "WHERE c_id = " + cid + " AND c_d_id = " + did + " AND c_w_id = " + wid);
            while (rs.next()) {
                discount = rs.getInt("c_discount");
                last_name = rs.getString("c_last");
                credit = rs.getString("c_credit");
                break;
            }

            double warehouse_tax_rate = 0;
            rs = stmt.executeQuery("SELECT w_tax FROM warehouse_tab WHERE w_id = " + wid);
            while (rs.next()) {
                warehouse_tax_rate = rs.getInt("w_tax");
                break;
            }

            double district_tax_rate = 0;
            rs = stmt.executeQuery("SELECT d_tax FROM district_tab WHERE d_id = " + did + " AND d_w_id = " + wid);
            while (rs.next()) {
                district_tax_rate = rs.getInt("d_tax");
                break;
            }
            rs.close();

            total_amount = total_amount * (1 + warehouse_tax_rate + district_tax_rate) * (1 - discount);
            conn.commit();

            System.out.printf("============================ New Order Transactions ============================ \n");
            System.out.printf("WarehouseID %d, DistrictID: %d, CustomerID: %d \n", wid, did, cid);
            System.out.printf("LastName %s, Credit: %s, Discount: %s \n", last_name, credit, discount);
            System.out.printf("WarehouseTaxRate %s, DistrictTaxRate: %s \n", warehouse_tax_rate, district_tax_rate);
            System.out.printf("OrderID %s, OrderEntryDate: %s, NumberOfItems \n", next_order_id, "", number_of_items);
            System.out.printf("TotalAmount %s \n", total_amount);
            for (int idx = 0; idx < items.size(); idx++) {
                System.out.printf(" ItemID %s, SupplierWarehouse %s, Quantity %s \n", items.get(idx), supplier_warehouses.get(idx), quantities.get(idx));
                System.out.printf(" ItemName %s, ItemAmount %s, StockQuantity %s \n", itemNames.get(idx), itemPrices.get(idx) * quantities.get(idx), itemStocks.get(idx));
            }
            System.out.printf("=============================================================================== \n");
            System.out.println();

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.printf("Create Order Error! sql state = [%s]cause = [%s]message = [%s]", e.getSQLState(), e.getCause(),
                    e.getMessage());
        }
    }

    private static void paymentTransaction(Connection conn, int cwid, int cdid, int cid, BigDecimal payment) {
        try {
            PreparedStatement updateWarehouse = conn.prepareStatement(
                    "UPDATE warehouse_tab SET W_YTD = W_YTD + ? WHERE W_ID = ?;");
            updateWarehouse.setBigDecimal(1, payment);
            updateWarehouse.setInt(2, cwid);
            updateWarehouse.executeUpdate();

            PreparedStatement updateDistrict = conn.prepareStatement(
                    "UPDATE district_tab SET D_YTD = D_YTD + ? WHERE  D_W_ID = ? AND D_ID=?");
            updateDistrict.setBigDecimal(1, payment);
            updateDistrict.setInt(2, cwid);
            updateDistrict.setInt(3, cdid);
            updateDistrict.executeUpdate();

            PreparedStatement updateCustomer = conn.prepareStatement(
                    "UPDATE customer_tab " +
                            "SET C_BALANCE= C_BALANCE - ?, C_YTD_PAYMENT= C_YTD_PAYMENT + ?, C_PAYMENT_CNT= C_PAYMENT_CNT + 1 " +
                            "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
            updateCustomer.setBigDecimal(1, payment);
            updateCustomer.setBigDecimal(2, payment);
            updateCustomer.setInt(3, cwid);
            updateCustomer.setInt(4, cdid);
            updateCustomer.setInt(5, cid);
            updateCustomer.executeUpdate();

            conn.commit();

        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]", e.getSQLState(), e.getCause(),
                    e.getMessage());
        }
    }

    private static void deliveryTransaction(Connection conn, int wid, int carrierid) {
        System.out.printf("Delivery wid:%d carrier:%d", wid, carrierid);
        try {
            for (int did = 1; did <= 10; did++) {
                // get the yet-to-deliver order with its client id
                PreparedStatement getOrderAndCustomer = conn.prepareStatement(
                        "SELECT o_id, o_c_id " +
                                "FROM order_tab " +
                                "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id ISNULL " +
                                "ORDER BY o_id " +
                                "LIMIT 1"
                );
                getOrderAndCustomer.setInt(1, wid);
                getOrderAndCustomer.setInt(2, did);
                ResultSet orderCustomerIdRS = getOrderAndCustomer.executeQuery();

                // proceed only when a yet-to-deliver order exists
                if (orderCustomerIdRS.next()) {
                    int oid = orderCustomerIdRS.getInt("o_id");
                    int cid = orderCustomerIdRS.getInt("o_c_id");
                    System.out.printf("oid:%d cid:%d\n", oid, cid);

                    // assign the carrier id to the order
                    PreparedStatement updateOrder = conn.prepareStatement(
                            "UPDATE order_tab " +
                                    "SET o_carrier_id = ? " +
                                    "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?"
                    );
                    updateOrder.setInt(1, carrierid);
                    updateOrder.setInt(2, wid);
                    updateOrder.setInt(3, did);
                    updateOrder.setInt(4, oid);
                    updateOrder.executeUpdate();

                    // assign the current timestamp to each order line
                    PreparedStatement updateOrderLine = conn.prepareStatement(
                            "UPDATE order_line_tab " +
                                    "SET ol_delivery_d = CURRENT_TIMESTAMP " +
                                    "WHERE ol_o_id = ? AND ol_w_id = ? AND ol_d_id = ?"
                    );
                    updateOrderLine.setInt(1, oid);
                    updateOrderLine.setInt(2, wid);
                    updateOrderLine.setInt(3, did);
                    updateOrderLine.executeUpdate();

                    // get the amount sum of all order lines
                    PreparedStatement getOrderLineSum = conn.prepareStatement(
                            "SELECT SUM(ol_amount) " +
                                    "FROM order_line_tab " +
                                    "WHERE ol_o_id = ? AND ol_w_id = ? AND ol_d_id = ?"
                    );
                    getOrderLineSum.setInt(1, oid);
                    getOrderLineSum.setInt(2, wid);
                    getOrderLineSum.setInt(3, did);
                    ResultSet orderLineSumRS = getOrderLineSum.executeQuery();

                    // parse the sum
                    orderLineSumRS.next();
                    BigDecimal orderAmountSum = orderLineSumRS.getBigDecimal("sum");
//                    System.out.printf("ordersum:%.2f", orderAmountSum);


                    // update the customer's balance and delivery count
                    PreparedStatement updateCustomerInfo = conn.prepareStatement(
                            "UPDATE customer_tab " +
                                    "SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + ? " +
                                    "WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?"
                    );
                    updateCustomerInfo.setBigDecimal(1, orderAmountSum);
                    updateCustomerInfo.setInt(2, 1);
                    updateCustomerInfo.setInt(3, cid);
                    updateCustomerInfo.setInt(4, wid);
                    updateCustomerInfo.setInt(5, did);
                    updateCustomerInfo.executeUpdate();
                }
            }
            conn.commit();
        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
    }

    private static void orderStatusTransaction(Connection conn, int cwid, int cdid, int cid) {
        String get_customer_last_order = "SELECT c_first, c_middle, c_last, c_balance, o_w_id, o_d_id, o_c_id, o_id, o_entry_d, o_carrier_id "
                + "FROM customer_tab, order_tab WHERE c_id = o_c_id AND c_d_id = o_d_id AND c_w_id = o_w_id "
                + "AND c_w_id = %d AND c_d_id = %d AND c_id = %d order by o_id desc LIMIT 1 ";
        String get_order_items = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line_tab "
                + "where ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d ";

        try {
            Statement st = conn.createStatement();

            //Customer Last Order
            ResultSet rs_customer_last_order = st.executeQuery(String.format(get_customer_last_order, cwid, cdid, cid));

            if (rs_customer_last_order.next()) {
                int last_order_id = rs_customer_last_order.getInt("o_id");

                System.out.printf("Customer's name: %s %s %s, balance: %f\n",
                        rs_customer_last_order.getString("c_first"),
                        rs_customer_last_order.getString("c_middle"),
                        rs_customer_last_order.getString("c_last"),
                        rs_customer_last_order.getBigDecimal("c_balance").doubleValue());

                System.out.printf("last order id: %d, entry datetime: %s, carrier id: %d\n",
                        last_order_id,
                        rs_customer_last_order.getString("o_entry_d"),
                        rs_customer_last_order.getInt("o_carrier_id"));

                //Order items
                ResultSet rs_order_items = st.executeQuery(String.format(get_order_items, cwid, cdid, last_order_id));

                while (rs_order_items.next()) {
                    System.out.printf("item id: %d, warehouse id: %d, quantity: %d, price: %d, delivery datetime: %s\n",
                            rs_order_items.getInt("ol_i_id"),
                            rs_order_items.getInt("ol_supply_w_id"),
                            rs_order_items.getInt("ol_quantity"),
                            rs_order_items.getInt("ol_amount"),
                            rs_order_items.getString("ol_delivery_d"));
                    System.out.println();
                }
                rs_order_items.close();

            }
            rs_customer_last_order.close();


        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]", e.getSQLState(), e.getCause(),
                    e.getMessage());
        }
    }

    private static void stockLevelTransaction(Connection conn, int wid, int did, int threshold, int l) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT d_next_o_id FROM district_tab WHERE d_w_id=" + wid + " AND d_id=" + did + ";");
            Integer latest_order_id = 0;
            while (rs.next()) {
                latest_order_id = rs.getInt("d_next_o_id");
            }

            Integer earliest_order_id = latest_order_id - l;
            PreparedStatement getOrderLine = conn.prepareStatement("SELECT ol_i_id FROM order_line_tab " +
                    "WHERE ol_d_id=? AND ol_w_id=? AND ol_o_id>? AND ol_o_id<?;");
            getOrderLine.setInt(1, did);
            getOrderLine.setInt(2, wid);
            getOrderLine.setInt(3, earliest_order_id);
            getOrderLine.setInt(4, latest_order_id);
            rs = getOrderLine.executeQuery();

            int low_stock_count = 0;
            while (rs.next()) {
                int item = rs.getInt("ol_i_id");
                rs = stmt.executeQuery("SELECT s_quantity FROM stock_tab WHERE s_w_id=" + wid + " AND s_i_id=" + item);
                rs.next();
                double quantity = rs.getInt("s_quantity");
                if (quantity < threshold) {
                    low_stock_count += 1;
                }
            }

            System.out.printf("============================ Stock Level Transaction ============================ \n");
            System.out.printf("Searching at Warehouse %d, District %d, Threshold %d, Last %d items \n", wid, did, threshold, l);
            System.out.printf("Total number of low stock level items is %d \n", low_stock_count);
            System.out.printf("================================================================================= \n");
        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }
    }

    private static void popularItemTransaction(Connection conn, int wid, int did, int l) {

        // get order join with customer
        String get_order_customer = "select o_id, o_d_id, o_w_id, o_entry_d, o_c_id, c_first, c_middle, c_last "
                + "from order_tab, customer_tab "
                + "where o_w_id = c_w_id and o_d_id = c_d_id and o_c_id = c_id "
                + "and o_w_id = %d and o_d_id = %d "
                + "order by o_id  desc limit %d ";

        // get maximum ol_quantity
        String get_ol_quantity_sum = "select ol_sum.ol_w_id, ol_sum.ol_d_id, ol_sum.ol_o_id, sum(ol_sum.ol_quantity) as sum_quantity "
                + "from order_line_tab ol_sum "
                + "where ol_sum.ol_w_id = %d and ol_sum.ol_d_id = %d "
                + "group by ol_sum.ol_w_id, ol_sum.ol_d_id, ol_sum.ol_o_id, ol_sum.ol_i_id";

        String get_ol_quantity_max = "select ol_max.ol_o_id, max(ol_max.sum_quantity) "
                + "from ( "
                + String.format(get_ol_quantity_sum, wid, did)
                + " ) ol_max "
                + "group by ol_max.ol_w_id, ol_max.ol_d_id, ol_max.ol_o_id ";

        // get popular items
        String get_popular_items = "select ol.ol_o_id, o.o_entry_d, CONCAT(c_first, ' ', c_middle, ' ', c_last) as c_name, sum(ol_quantity) as quantity, i.i_name, i.i_id "
                + "from order_line_tab ol "
                + "join ("
                + String.format(get_order_customer, wid, did, l)
                + ") o on ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id "
                + "join item_tab i ON i.i_id = ol.ol_i_id "
                + "group by ol.ol_o_id, o.o_entry_d, CONCAT(o.c_first, ' ', o.c_middle, ' ', o.c_last), i.i_id, i.i_name "
                + "having (ol.ol_o_id, sum(ol.ol_quantity)) in ("
                + get_ol_quantity_max
                + ")";

        try {
            Statement st = conn.createStatement();
            ResultSet rs_popular_items = st.executeQuery(String.format(get_popular_items, wid, did, l));

            Map<Integer, ArrayList<String>> orders = new HashMap<Integer, ArrayList<String>>();
            Map<Integer, Set<Integer>> items = new HashMap<Integer, Set<Integer>>();
            Map<Integer, String> order_descs = new HashMap<Integer, String>();
            Map<Integer, String> item_descs = new HashMap<Integer, String>();

            while (rs_popular_items.next()) {
                int oid = rs_popular_items.getInt("ol_o_id");
                int iid = rs_popular_items.getInt("i_id");
                String iname = rs_popular_items.getString("i_name");
                String popular_item_desc = String.format("Popular I_NAME: %s, quantity: %d\n", iname, rs_popular_items.getInt("quantity"));
                String order_customer_desc = String.format("OID: %d, O_ENTRY_D: %s, Customer Name: %s\n", oid, rs_popular_items.getString("o_entry_d"), rs_popular_items.getString("c_name"));
                // get popular items for each order
                if (!orders.containsKey(oid)) {
                    ArrayList<String> popular_items = new ArrayList<String>();
                    popular_items.add(popular_item_desc);
                    orders.put(oid, popular_items);
                    order_descs.put(oid, order_customer_desc);
                } else {
                    ArrayList<String> popular_items = orders.get(oid);
                    popular_items.add(popular_item_desc);
                    orders.put(oid, popular_items);
                }

                // get order count for each item
                if (!items.containsKey(iid)) {
                    Set<Integer> oids = new HashSet<Integer>();
                    oids.add(oid);
                    items.put(iid, oids);
                    item_descs.put(iid, iname);
                } else {
                    Set<Integer> oids = items.get(iid);
                    oids.add(oid);
                    items.put(iid, oids);
                }

            }

            // Output results
            System.out.printf("WID: %d, DID: %d, Number of last orders: %d\n", wid, did, l);
            System.out.println();

            for (int oid : orders.keySet()) {
                System.out.printf(order_descs.get(oid));
                for (String desc : orders.get(oid)) {
                    System.out.printf(desc);
                }
                System.out.println();
            }

            for (int iid : items.keySet()) {
                System.out.printf("Popular I_NAME: %s, Percentage of Orders having Popular Items: %f\n", item_descs.get(iid), (float) items.get(iid).size() * 1 / orders.size());
            }

            rs_popular_items.close();

        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]", e.getSQLState(), e.getCause(),
                    e.getMessage());
        }
    }

    private static void topBalanceTransaction(Connection conn) {
        System.out.println("Customers with top balances");
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT\n" +
                            "  c.w_id,\n" +
                            "  c.d_id,\n" +
                            "  c.c_first,\n" +
                            "  c.c_middle,\n" +
                            "  c.c_last,\n" +
                            "  c.c_balance,\n" +
                            "  d.d_name,\n" +
                            "  w.w_name\n" +
                            "FROM\n" +
                            "  (\n" +
                            "    (\n" +
                            "      SELECT\n" +
                            "        c_w_id AS w_id,\n" +
                            "        c_d_id AS d_id,\n" +
                            "        c_first,\n" +
                            "        c_middle,\n" +
                            "        c_last,\n" +
                            "        c_balance\n" +
                            "      FROM\n" +
                            "        customer_tab\n" +
                            "      ORDER BY\n" +
                            "        c_balance DESC\n" +
                            "      LIMIT\n" +
                            "        10\n" +
                            "    ) AS c\n" +
                            "    JOIN district_tab AS d ON c.w_id = d.d_w_id\n" +
                            "    AND c.d_id = d.d_id\n" +
                            "  )\n" +
                            "  JOIN warehouse_tab AS w ON c.w_id = w.w_id\n" +
                            "ORDER BY\n" +
                            "  c_balance DESC"
            );

            while (rs.next()) {
                String firstName = rs.getString("c_first");
                String middleName = rs.getString("c_middle");
                String lastName = rs.getString("c_last");
                BigDecimal balance = rs.getBigDecimal("c_balance");
                String warehouseName = rs.getString("w_name");
                String districtName = rs.getString("d_name");

                System.out.printf("Customer Name: %-36s\tBalance: %-12.2f\tWarehouse Name: %-10s\tDistrict Name: %-10s\n",
                        firstName + ' ' + middleName + ' ' + lastName, balance, warehouseName, districtName);
            }
        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }

    }

    private static void relatedCustomerTransaction(Connection conn, int cwid, int cdid, int cid) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT DISTINCT w_id_1 as c_w_id, d_id_1 as c_d_id, c_id_1 as c_id " +
                            "FROM (SELECT o_w_id as w_id_1, o_c_id as c_id_1,  o_d_id as d_id_1, ol_i_id as i_1" +
                            "      FROM order_tab  ,order_line_tab " +
                            "      WHERE o_w_id = ol_w_id AND o_d_id = ol_d_id AND o_id = ol_o_id AND o_w_id <> " + cwid + ") AS a, " +
                            "     (SELECT o_w_id as w_id_2, o_c_id as c_id_2,  o_d_id as d_id_2, ol_i_id as i_2" +
                            "      FROM order_tab  ,order_line_tab " +
                            "      WHERE o_w_id = ol_w_id AND o_d_id = ol_d_id AND o_id = ol_o_id AND o_w_id <> " + cwid + ") AS b " +
                            "WHERE w_id_1 = w_id_2 AND c_id_1 = c_id_2 AND d_id_1 = d_id_2 AND i_1 <> i_2 " +
                            "" +
                            "AND i_1 IN (SELECT DISTINCT ol_i_id as c_items " +
                            "            FROM order_tab , order_line_tab " +
                            "            WHERE o_w_id = ol_w_id " +
                            "            AND o_d_id = ol_d_id " +
                            "            AND o_id = ol_o_id " +
                            "            AND o_w_id = " + cwid +
                            "            AND o_d_id = " + cdid +
                            "            AND o_c_id = " + cid + ")" +
                            "" +
                            "AND i_2 IN (SELECT DISTINCT ol_i_id as c_items " +
                            "            FROM order_tab , order_line_tab " +
                            "            WHERE o_w_id = ol_w_id " +
                            "            AND o_d_id = ol_d_id " +
                            "            AND o_id = ol_o_id " +
                            "            AND o_w_id = " + cwid +
                            "            AND o_d_id = " + cdid +
                            "            AND o_c_id = " + cid + ")"
            );

            while (rs.next()) {
                int r_cwid = rs.getInt(1);
                int r_cdid = rs.getInt(2);
                int r_cid = rs.getInt(3);

                System.out.printf("CWID: %d, C_DID: %d, C_ID: %d", r_cwid, r_cdid, r_cid);
                System.out.println();
            }
            rs.close();

        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }

    }

}
