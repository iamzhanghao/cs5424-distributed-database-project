package clients;

import clients.utils.CockroachDbSQLConnectionHelper;
import clients.utils.Customer;
import clients.utils.Order;
import clients.utils.TransactionStatistics;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;

public class CockroachDB {

    // Limit number of transactions executed, during actual experiment change to
    // 20000
    private static int TXN_LIMIT = 200000;
    private static final int MAX_RETRY_COUNT = 100000000;
    private static final int RETRY_QUERY_AFTER = 200;

    private static CockroachDbSQLConnectionHelper connHelper;

    private static int getDelay(int count) {
        int random = 100 + (int) (Math.random() * ((RETRY_QUERY_AFTER - 100) + 1));
        System.out.printf("Retry count= %d, retry in %d milliseconds", count, random);
        return random;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println(
                    "run the program by: ./CockroachDB <host> <port> <schema_name> <client> <csv_path> <is_db_state>\n "
                            + "e.g. ./CockroachDB localhost 26267 A 1 out/cockroachdb-A-local.csv 0");
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String schema = args[2];
        String client = args[3];
        String csvPath = args[4];
        int isDbState = Integer.parseInt(args[5]);

        String schema_name = "schema_a";
        String dataDir = "project_files/xact_files_A/1.txt";

        if (Objects.equals(schema, "A")) {
            schema_name = "schema_a";
            dataDir = "project_files/xact_files_A/" + client + ".txt";
        } else if (Objects.equals(schema, "B")) {
            schema_name = "schema_b";
            dataDir = "project_files/xact_files_B/" + client + ".txt";
        } else {
            System.err.println("run the program by: ./CockroachDB <host> <port> <schema_name> <client>\n "
                    + "e.g. ./CockroachDB localhost 26267 A 1 out/cockroachdb-A-local.csv 0");
            return;
        }

        if (isDbState == 1) {
            TXN_LIMIT = 0;
        }

        System.out.printf("Running on host: %s:%d", host, port);
        TransactionStatistics.printServerTime();
        System.out.println();

        connHelper = new CockroachDbSQLConnectionHelper(host, port, schema_name);

        FileInputStream stream = new FileInputStream(dataDir);
        Scanner scanner = new Scanner(stream);

        System.out.println("Ready to read Xact file " + dataDir);

        // Client 0 always write CSV header
        if (client.equals("0")) {
            TransactionStatistics.writeCsvHeader(csvPath);
        }

        if (isDbState == 1) {
            TXN_LIMIT = 0;
        }

        ArrayList<TransactionStatistics> latencies = new ArrayList<>();
        int txnCount = 0;
        long clientStartTime = System.currentTimeMillis();
        while (scanner.hasNextLine() && (txnCount < TXN_LIMIT)) {
            long start = System.nanoTime();
            txnCount++;
            String line = scanner.nextLine();
            String[] splits = line.split(",");
            char txnType = splits[0].toCharArray()[0];
            int retryCount = invokeTransaction(splits, scanner);
            float latency = System.nanoTime() - start;
            latencies.add(new TransactionStatistics(txnType, (float) latency / 1000000, (float) retryCount));
            System.out.printf("<%d/20000> Tnx %c: %.2fms, retry: %d times \n", txnCount, txnType, latency / 1000000,
                    retryCount);
        }
        float clientTotalTime = (float) (System.currentTimeMillis() - clientStartTime) / 1000;
        String message = TransactionStatistics.getStatistics(latencies, clientTotalTime, client, csvPath);
        if (isDbState == 1) {
            while (true) {
                try {
                    getDbState();
                } catch (SQLException e) {
                    System.out.println("RETRY DB STATE in 2 seconds");
                    if (e.getSQLState().equals("08003")) {
                        connHelper.connect();
                    }
                }
                Thread.sleep(2000);
                break;
            }
        }
        connHelper.close();
        System.out.println();
        System.err.println(message);
    }

    private static int invokeTransaction(String[] splits, Scanner scanner) {
        int retryCount = 0;
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
                for (int i = 0; i < MAX_RETRY_COUNT; i++) {
                    try {
                        newOrderTransaction(cid, wid, did, numItems, items, supplierWarehouses, quantities);
                        break;
                    } catch (SQLException e) {
                        System.out.printf("SQL Error! sql state = [%s]cause = [%s]message = [%s]\n", e.getSQLState(),
                                e.getCause(), e.getMessage());
                        if (e.getSQLState().equals("08003")) {
                            connHelper.connect();
                        }
                    }
                    retryCount++;
                    System.out.println("ROLLBACK AND RETRY");
                    try {
                        connHelper.getConn().rollback();
                        Thread.sleep(getDelay(i));
                    } catch (Exception e) {
                    }
                }
                break;

            case 'P':
                int cwid = Integer.parseInt(splits[1]);
                int cdid = Integer.parseInt(splits[2]);
                cid = Integer.parseInt(splits[3]);
                BigDecimal payment = new BigDecimal(splits[4]);
                for (int i = 0; i < MAX_RETRY_COUNT; i++) {
                    try {
                        paymentTransaction(cwid, cdid, cid, payment);
                        break;
                    } catch (SQLException e) {
                        System.out.printf("SQL Error! sql state = [%s]cause = [%s]message = [%s]\n", e.getSQLState(),
                                e.getCause(), e.getMessage());
                        if (e.getSQLState().equals("08003")) {
                            connHelper.connect();
                        }
                    }
                    retryCount++;
                    System.out.println("ROLLBACK AND RETRY");
                    try {
                        connHelper.getConn().rollback();
                        Thread.sleep(getDelay(i));
                    } catch (Exception e) {
                    }
                }
                break;

            case 'D':
                wid = Integer.parseInt(splits[1]);
                int carrierid = Integer.parseInt(splits[2]);
                for (int i = 0; i < MAX_RETRY_COUNT; i++) {
                    try {
                        deliveryTransaction(wid, carrierid);
                        break;
                    } catch (SQLException e) {
                        System.out.printf("SQL Error! sql state = [%s]cause = [%s]message = [%s]\n", e.getSQLState(),
                                e.getCause(), e.getMessage());
                        if (e.getSQLState().equals("08003")) {
                            connHelper.connect();
                        }
                    }
                    retryCount++;
                    System.out.println("ROLLBACK AND RETRY");
                    try {
                        connHelper.getConn().rollback();
                        Thread.sleep(getDelay(i));
                    } catch (Exception e) {
                    }
                }
                break;

            case 'O':
                cwid = Integer.parseInt(splits[1]);
                cdid = Integer.parseInt(splits[2]);
                cid = Integer.parseInt(splits[3]);
                for (int i = 0; i < MAX_RETRY_COUNT; i++) {
                    try {
                        orderStatusTransaction(cwid, cdid, cid);
                        break;
                    } catch (SQLException e) {
                        System.out.printf("SQL Error! sql state = [%s]cause = [%s]message = [%s]\n", e.getSQLState(),
                                e.getCause(), e.getMessage());
                        if (e.getSQLState().equals("08003")) {
                            connHelper.connect();
                        }
                    }
                    retryCount++;
                    System.out.println("ROLLBACK AND RETRY");
                    try {
                        connHelper.getConn().rollback();
                        Thread.sleep(getDelay(i));
                    } catch (Exception e) {
                    }
                }
                break;

            case 'S':
                wid = Integer.parseInt(splits[1]);
                did = Integer.parseInt(splits[2]);
                int t = Integer.parseInt(splits[3]);
                int l = Integer.parseInt(splits[4]);
                for (int i = 0; i < MAX_RETRY_COUNT; i++) {
                    try {
                        stockLevelTransaction(wid, did, t, l);
                        break;
                    } catch (SQLException e) {
                        System.out.printf("SQL Error! sql state = [%s]cause = [%s]message = [%s]\n", e.getSQLState(),
                                e.getCause(), e.getMessage());
                        if (e.getSQLState().equals("08003")) {
                            connHelper.connect();
                        }
                    }
                    retryCount++;
                    System.out.println("ROLLBACK AND RETRY");
                    try {
                        connHelper.getConn().rollback();
                        Thread.sleep(getDelay(i));
                    } catch (Exception e) {
                    }
                }
                break;

            case 'I':
                wid = Integer.parseInt(splits[1]);
                did = Integer.parseInt(splits[2]);
                l = Integer.parseInt(splits[3]);
                for (int i = 0; i < MAX_RETRY_COUNT; i++) {
                    try {
                        popularItemTransaction(wid, did, l);
                        break;
                    } catch (SQLException e) {
                        System.out.printf("SQL Error! sql state = [%s]cause = [%s]message = [%s]\n", e.getSQLState(),
                                e.getCause(), e.getMessage());
                        if (e.getSQLState().equals("08003")) {
                            connHelper.connect();
                        }
                    }
                    retryCount++;
                    System.out.println("ROLLBACK AND RETRY");
                    try {
                        connHelper.getConn().rollback();
                        Thread.sleep(getDelay(i));
                    } catch (Exception e) {
                    }
                }
                break;

            case 'T':
                for (int i = 0; i < MAX_RETRY_COUNT; i++) {
                    try {
                        topBalanceTransaction();
                        break;
                    } catch (SQLException e) {
                        System.out.printf("SQL Error! sql state = [%s]cause = [%s]message = [%s]\n", e.getSQLState(),
                                e.getCause(), e.getMessage());
                        if (e.getSQLState().equals("08003")) {
                            connHelper.connect();
                        }
                    }
                    retryCount++;
                    System.out.println("ROLLBACK AND RETRY");
                    try {
                        connHelper.getConn().rollback();
                        Thread.sleep(getDelay(i));
                    } catch (Exception e) {
                    }
                }
                break;

            case 'R':
                cwid = Integer.parseInt(splits[1]);
                cdid = Integer.parseInt(splits[2]);
                cid = Integer.parseInt(splits[3]);
                for (int i = 0; i < MAX_RETRY_COUNT; i++) {
                    try {
                        relatedCustomerTransaction(cwid, cdid, cid);
                        break;
                    } catch (SQLException e) {
                        System.out.printf("SQL Error! sql state = [%s]cause = [%s]message = [%s]\n", e.getSQLState(),
                                e.getCause(), e.getMessage());
                        if (e.getSQLState().equals("08003")) {
                            connHelper.connect();
                        }
                    }
                    retryCount++;
                    System.out.println("ROLLBACK AND RETRY");
                    try {
                        connHelper.getConn().rollback();
                        Thread.sleep(getDelay(i));
                    } catch (Exception e) {
                    }
                }
                break;
        }
        return retryCount;
    }

    private static void newOrderTransaction(int cid, int wid, int did, int number_of_items, ArrayList<Integer> items,
                                            ArrayList<Integer> supplier_warehouses, ArrayList<Integer> quantities) throws SQLException {
        Statement stmt = connHelper.getConn().createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM district_tab WHERE D_W_ID = " + wid + " AND D_ID = " + did);
        Integer next_order_id = 0;
        while (rs.next()) {
            next_order_id = rs.getInt("D_NEXT_O_ID");
            break;
        }

        next_order_id += 1;

        PreparedStatement updateDistrict = connHelper.getConn()
                .prepareStatement("UPDATE district_tab SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?");
        updateDistrict.setInt(1, next_order_id);
        updateDistrict.setInt(2, wid);
        updateDistrict.setInt(3, did);
        updateDistrict.executeUpdate();

        int all_local = 0;
        if (supplier_warehouses.stream().distinct().count() <= 1 && supplier_warehouses.contains(wid)) {
            all_local = 1;
        }

        PreparedStatement createOrder = connHelper.getConn()
                .prepareStatement("UPSERT INTO order_tab (O_W_ID, O_D_ID, O_ID, O_C_ID,"
                        + " O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (?,?,?,?,?,?,?,?)");
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

            PreparedStatement itemStock = connHelper.getConn().prepareStatement("SELECT s_quantity,s_ytd,s_order_cnt,"
                    + "s_remote_cnt, s_dist_" + strDid + " FROM stock_tab WHERE s_w_id=? AND s_i_id=?;");

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

            PreparedStatement updateStock = connHelper.getConn().prepareStatement("UPDATE stock_tab SET "
                    + "s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE s_w_id=? AND s_i_id=?;");
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

            PreparedStatement insertOrderLine = connHelper.getConn()
                    .prepareStatement("UPSERT INTO order_line_tab (OL_W_ID, OL_D_ID, "
                            + "OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) "
                            + "VALUES (?,?,?,?,?,?,?,?,?,?);");
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
        double warehouse_tax_rate = 0;
        double district_tax_rate = 0;

        PreparedStatement get_user_info = connHelper.getConn()
                .prepareStatement("select c_last, c_credit, c_discount, w_tax, d_tax from "
                        + "warehouse_district_customer where w_id=? and d_id=? and c_id=?;");
        get_user_info.setInt(1, wid);
        get_user_info.setInt(2, did);
        get_user_info.setInt(3, cid);
        rs = get_user_info.executeQuery();
        rs.next();
        last_name = rs.getString("c_last");
        discount = rs.getInt("c_discount");
        credit = rs.getString("c_credit");
        warehouse_tax_rate = rs.getInt("w_tax");
        district_tax_rate = rs.getInt("d_tax");

        // rs = stmt.executeQuery("SELECT c_last, c_credit, c_discount FROM customer_tab
        // " +
        // "WHERE c_id = " + cid + " AND c_d_id = " + did + " AND c_w_id = " + wid);
        // while (rs.next()) {
        // discount = rs.getInt("c_discount");
        // last_name = rs.getString("c_last");
        // credit = rs.getString("c_credit");
        // break;
        // }

        // double warehouse_tax_rate = 0;
        // rs = stmt.executeQuery("SELECT w_tax FROM warehouse_tab WHERE w_id = " +
        // wid);
        // while (rs.next()) {
        // warehouse_tax_rate = rs.getInt("w_tax");
        // break;
        // }

        // double district_tax_rate = 0;
        // rs = stmt.executeQuery("SELECT d_tax FROM district_tab WHERE d_id = " + did +
        // " AND d_w_id = " + wid);
        // while (rs.next()) {
        // district_tax_rate = rs.getInt("d_tax");
        // break;
        // }
        rs.close();

        total_amount = total_amount * (1 + warehouse_tax_rate + district_tax_rate) * (1 - discount);
        connHelper.getConn().commit();

        System.out.printf("============================ New Order Transactions ============================ \n");
        System.out.printf("WarehouseID %d, DistrictID: %d, CustomerID: %d \n", wid, did, cid);
        System.out.printf("LastName %s, Credit: %s, Discount: %s \n", last_name, credit, discount);
        System.out.printf("WarehouseTaxRate %s, DistrictTaxRate: %s \n", warehouse_tax_rate, district_tax_rate);
        System.out.printf("OrderID %s, OrderEntryDate: %s, NumberOfItems \n", next_order_id, "", number_of_items);
        System.out.printf("TotalAmount %s \n", total_amount);
        for (int idx = 0; idx < items.size(); idx++) {
            System.out.printf(" ItemID %s, SupplierWarehouse %s, Quantity %s \n", items.get(idx),
                    supplier_warehouses.get(idx), quantities.get(idx));
            System.out.printf(" ItemName %s, ItemAmount %s, StockQuantity %s \n", itemNames.get(idx),
                    itemPrices.get(idx) * quantities.get(idx), itemStocks.get(idx));
        }
        System.out.printf("=============================================================================== \n");
        System.out.println();
    }

    private static void paymentTransaction(int cwid, int cdid, int cid, BigDecimal payment) throws SQLException {
        PreparedStatement updateWarehouse = connHelper.getConn()
                .prepareStatement("UPDATE warehouse_tab SET W_YTD = W_YTD + ? WHERE W_ID = ?;");
        updateWarehouse.setBigDecimal(1, payment);
        updateWarehouse.setInt(2, cwid);
        updateWarehouse.executeUpdate();

        PreparedStatement updateDistrict = connHelper.getConn()
                .prepareStatement("UPDATE district_tab SET D_YTD = D_YTD + ? WHERE  D_W_ID = ? AND D_ID=?");
        updateDistrict.setBigDecimal(1, payment);
        updateDistrict.setInt(2, cwid);
        updateDistrict.setInt(3, cdid);
        updateDistrict.executeUpdate();

        PreparedStatement updateCustomer = connHelper.getConn().prepareStatement("UPDATE customer_tab "
                + "SET C_BALANCE= C_BALANCE - ?, C_YTD_PAYMENT= C_YTD_PAYMENT + ?, C_PAYMENT_CNT= C_PAYMENT_CNT + 1 "
                + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
        updateCustomer.setBigDecimal(1, payment);
        updateCustomer.setBigDecimal(2, payment);
        updateCustomer.setInt(3, cwid);
        updateCustomer.setInt(4, cdid);
        updateCustomer.setInt(5, cid);
        updateCustomer.executeUpdate();

        Statement stmt = connHelper.getConn().createStatement();

        ResultSet rs = stmt.executeQuery(String.format(
                "SELECT (c_w_id, c_d_id, c_id) as identifier, ( c_first, c_middle, c_last) as name,\n"
                        + " (C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP) as address , C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE\n"
                        + " FROM customer_tab \n" + "WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d",
                cwid, cdid, cid));

        System.out.println("Customer payment");
        while (rs.next()) {
            System.out.printf(
                    "Customer %s, name: %s, address: %s\n phone %s, c_since %s, c_credit %s, c_credit_lim %f, "
                            + " c_discount %f, c_balance %f\n",
                    rs.getString("identifier"), rs.getString("name"), rs.getString("address"), rs.getString("c_phone"),
                    rs.getString("c_since"), rs.getString("c_credit"), rs.getBigDecimal("c_credit_lim"),
                    rs.getBigDecimal("c_discount"), rs.getBigDecimal("c_balance"));
        }

        rs = stmt.executeQuery(String.format(
                "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM warehouse_tab \n" + "WHERE w_id = %d",
                cwid));

        while (rs.next()) {
            System.out.printf("Warehouse: street_1: %s, street_2: %s, w_city %s, w_state %s, w_zip %s\n",
                    rs.getString("w_street_1"), rs.getString("w_street_2"), rs.getString("w_city"),
                    rs.getString("w_state"), rs.getString("w_zip"));
        }

        rs = stmt
                .executeQuery(String.format("SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM district_tab \n"
                        + "WHERE d_w_id = %d AND d_id = %d ", cwid, cdid));

        while (rs.next()) {
            System.out.printf("District address: d_street_1: %s, d_street_2: %s, d_city %s, d_state %s, d_zip %s\n",
                    rs.getString("d_street_1"), rs.getString("d_street_2"), rs.getString("d_city"),
                    rs.getString("d_state"), rs.getString("d_zip"));
        }

        System.out.printf("Payment amount: %f", payment.doubleValue());
        connHelper.getConn().commit();

    }

    private static void deliveryTransaction(int wid, int carrierid) throws SQLException {
        System.out.printf("Delivery wid:%d carrier:%d", wid, carrierid);
        for (int did = 1; did <= 10; did++) {
            // get the yet-to-deliver order with its client id
            PreparedStatement getOrderAndCustomer = connHelper.getConn()
                    .prepareStatement("SELECT o_id, o_c_id " + "FROM order_tab "
                            + "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id ISNULL " + "ORDER BY o_id "
                            + "LIMIT 1");
            getOrderAndCustomer.setInt(1, wid);
            getOrderAndCustomer.setInt(2, did);
            ResultSet orderCustomerIdRS = getOrderAndCustomer.executeQuery();

            // proceed only when a yet-to-deliver order exists
            if (orderCustomerIdRS.next()) {
                int oid = orderCustomerIdRS.getInt("o_id");
                int cid = orderCustomerIdRS.getInt("o_c_id");
                System.out.printf("oid:%d cid:%d\n", oid, cid);

                // assign the carrier id to the order
                PreparedStatement updateOrder = connHelper.getConn().prepareStatement(
                        "UPDATE order_tab " + "SET o_carrier_id = ? " + "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
                updateOrder.setInt(1, carrierid);
                updateOrder.setInt(2, wid);
                updateOrder.setInt(3, did);
                updateOrder.setInt(4, oid);
                updateOrder.executeUpdate();

                // assign the current timestamp to each order line
                PreparedStatement updateOrderLine = connHelper.getConn()
                        .prepareStatement("UPDATE order_line_tab " + "SET ol_delivery_d = CURRENT_TIMESTAMP "
                                + "WHERE ol_o_id = ? AND ol_w_id = ? AND ol_d_id = ?");
                updateOrderLine.setInt(1, oid);
                updateOrderLine.setInt(2, wid);
                updateOrderLine.setInt(3, did);
                updateOrderLine.executeUpdate();

                // get the amount sum of all order lines
                PreparedStatement getOrderLineSum = connHelper.getConn().prepareStatement("SELECT SUM(ol_amount) "
                        + "FROM order_line_tab " + "WHERE ol_o_id = ? AND ol_w_id = ? AND ol_d_id = ?");
                getOrderLineSum.setInt(1, oid);
                getOrderLineSum.setInt(2, wid);
                getOrderLineSum.setInt(3, did);
                ResultSet orderLineSumRS = getOrderLineSum.executeQuery();

                // parse the sum
                orderLineSumRS.next();
                BigDecimal orderAmountSum = orderLineSumRS.getBigDecimal("sum");
                // System.out.printf("ordersum:%.2f", orderAmountSum);

                // update the customer's balance and delivery count
                PreparedStatement updateCustomerInfo = connHelper.getConn()
                        .prepareStatement("UPDATE customer_tab "
                                + "SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + ? "
                                + "WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?");
                updateCustomerInfo.setBigDecimal(1, orderAmountSum);
                updateCustomerInfo.setInt(2, 1);
                updateCustomerInfo.setInt(3, cid);
                updateCustomerInfo.setInt(4, wid);
                updateCustomerInfo.setInt(5, did);
                updateCustomerInfo.executeUpdate();
            }
        }
        connHelper.getConn().commit();
    }

    private static void orderStatusTransaction(int cwid, int cdid, int cid) throws SQLException {
        String get_customer_last_order = "SELECT c_first, c_middle, c_last, c_balance, o_w_id, o_d_id, o_c_id, o_id, o_entry_d, o_carrier_id "
                + "FROM customer_tab, order_tab WHERE c_id = o_c_id AND c_d_id = o_d_id AND c_w_id = o_w_id "
                + "AND c_w_id = %d AND c_d_id = %d AND c_id = %d order by o_id desc LIMIT 1 ";
        String get_order_items = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line_tab "
                + "where ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d ";

        //Customer Last Order
        ResultSet rs_customer_last_order = connHelper.getConn().prepareStatement(String.format(get_customer_last_order, cwid, cdid, cid)).executeQuery();

        if (rs_customer_last_order.next()) {
            int last_order_id = rs_customer_last_order.getInt("o_id");

            System.out.printf("Customer name: %s %s %s, Balance: %f\n", rs_customer_last_order.getString("c_first"),
                    rs_customer_last_order.getString("c_middle"), rs_customer_last_order.getString("c_last"),
                    rs_customer_last_order.getBigDecimal("c_balance").doubleValue());

            System.out.printf("Customer last order id: %d, Entry Datetime: %s, Carrier id: %d\n", last_order_id,
                    rs_customer_last_order.getString("o_entry_d"), rs_customer_last_order.getInt("o_carrier_id"));

            //Order items
            ResultSet rs_order_items = connHelper.getConn().prepareStatement(String.format(get_order_items, cwid, cdid, last_order_id)).executeQuery();

            while (rs_order_items.next()) {
                System.out.printf("Item id: %d, Warehouse id: %d, Quantity: %d, Price: %d, Delivery Datetime: %s\n",
                        rs_order_items.getInt("ol_i_id"), rs_order_items.getInt("ol_supply_w_id"),
                        rs_order_items.getInt("ol_quantity"), rs_order_items.getInt("ol_amount"),
                        rs_order_items.getString("ol_delivery_d"));
                System.out.println();
            }
            rs_order_items.close();

        }
        rs_customer_last_order.close();

    }

    private static void stockLevelTransaction(int wid, int did, int threshold, int l) throws SQLException {
        Statement stmt = connHelper.getConn().createStatement();
        ResultSet rs = stmt
                .executeQuery("SELECT d_next_o_id FROM district_tab WHERE d_w_id=" + wid + " AND d_id=" + did + ";");
        Integer latest_order_id = 0;
        while (rs.next()) {
            latest_order_id = rs.getInt("d_next_o_id");
        }

        Integer earliest_order_id = latest_order_id - l;
        PreparedStatement getOrderLine = connHelper.getConn().prepareStatement(
                "SELECT ol_i_id FROM order_line_tab " + "WHERE ol_d_id=? AND ol_w_id=? AND ol_o_id>? AND ol_o_id<?;");
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
        System.out.printf("Searching at Warehouse %d, District %d, Threshold %d, Last %d items \n", wid, did, threshold,
                l);
        System.out.printf("Total number of low stock level items is %d \n", low_stock_count);
        System.out.printf("================================================================================= \n");
    }

    private static void popularItemTransaction(int wid, int did, int l) throws SQLException {

        // get order join with customer
        String get_order_customer = "select o_id, o_d_id, o_w_id, o_entry_d, o_c_id, c_first, c_middle, c_last "
                + "from order_tab, customer_tab " + "where o_w_id = c_w_id and o_d_id = c_d_id and o_c_id = c_id "
                + "and o_w_id = %d and o_d_id = %d " + "order by o_id  desc limit %d ";

        // get maximum ol_quantity
        String get_ol_quantity_sum = "select ol_sum.ol_w_id, ol_sum.ol_d_id, ol_sum.ol_o_id, sum(ol_sum.ol_quantity) as sum_quantity "
                + "from order_line_tab ol_sum " + "where ol_sum.ol_w_id = %d and ol_sum.ol_d_id = %d "
                + "group by ol_sum.ol_w_id, ol_sum.ol_d_id, ol_sum.ol_o_id, ol_sum.ol_i_id";

        String get_ol_quantity_max = "select ol_max.ol_o_id, max(ol_max.sum_quantity) " + "from ( "
                + String.format(get_ol_quantity_sum, wid, did) + " ) ol_max "
                + "group by ol_max.ol_w_id, ol_max.ol_d_id, ol_max.ol_o_id ";

        // get popular items
        String get_popular_items = "select ol.ol_o_id, o.o_entry_d, CONCAT(c_first, ' ', c_middle, ' ', c_last) as c_name, sum(ol_quantity) as quantity, i.i_name, i.i_id "
                + "from order_line_tab ol " + "join (" + String.format(get_order_customer, wid, did, l)
                + ") o on ol.ol_w_id = o.o_w_id and ol.ol_d_id = o.o_d_id and ol.ol_o_id = o.o_id "
                + "join item_tab i ON i.i_id = ol.ol_i_id "
                + "group by ol.ol_o_id, o.o_entry_d, CONCAT(o.c_first, ' ', o.c_middle, ' ', o.c_last), i.i_id, i.i_name "
                + "having (ol.ol_o_id, sum(ol.ol_quantity)) in (" + get_ol_quantity_max + ")";

        ResultSet rs_popular_items = connHelper.getConn().prepareStatement(String.format(get_popular_items, wid, did, l)).executeQuery();

        Map<Integer, ArrayList<String>> orders = new HashMap<Integer, ArrayList<String>>();
        Map<Integer, Set<Integer>> items = new HashMap<Integer, Set<Integer>>();
        Map<Integer, String> order_descs = new HashMap<Integer, String>();
        Map<Integer, String> item_descs = new HashMap<Integer, String>();

        while (rs_popular_items.next()) {
            int oid = rs_popular_items.getInt("ol_o_id");
            int iid = rs_popular_items.getInt("i_id");
            String iname = rs_popular_items.getString("i_name");
            String popular_item_desc = String.format("Popular I_NAME: %s, quantity: %d\n", iname,
                    rs_popular_items.getInt("quantity"));
            String order_customer_desc = String.format("OID: %d, O_ENTRY_D: %s, Customer Name: %s\n", oid,
                    rs_popular_items.getString("o_entry_d"), rs_popular_items.getString("c_name"));
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
            System.out.printf("Popular I_NAME: %s, Percentage of Orders having Popular Items: %f\n",
                    item_descs.get(iid), (float) items.get(iid).size() * 1 / orders.size());
        }

        rs_popular_items.close();
    }

    private static void topBalanceTransaction() throws SQLException {
        System.out.println("Customers with top balances");
        Statement stmt = connHelper.getConn().createStatement();
        ResultSet rs = stmt.executeQuery("SELECT\n" + "  c.w_id,\n" + "  c.d_id,\n" + "  c.c_first,\n"
                + "  c.c_middle,\n" + "  c.c_last,\n" + "  c.c_balance,\n" + "  d.d_name,\n" + "  w.w_name\n" + "FROM\n"
                + "  (\n" + "    (\n" + "      SELECT\n" + "        c_w_id AS w_id,\n" + "        c_d_id AS d_id,\n"
                + "        c_first,\n" + "        c_middle,\n" + "        c_last,\n" + "        c_balance\n"
                + "      FROM\n" + "        customer_tab\n" + "      ORDER BY\n" + "        c_balance DESC\n"
                + "      LIMIT\n" + "        10\n" + "    ) AS c\n"
                + "    JOIN district_tab AS d ON c.w_id = d.d_w_id\n" + "    AND c.d_id = d.d_id\n" + "  )\n"
                + "  JOIN warehouse_tab AS w ON c.w_id = w.w_id\n" + "ORDER BY\n" + "  c_balance DESC");

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
    }

    private static void relatedCustomerTransaction(int cwid, int cdid, int cid) throws SQLException {
        Statement stmt = connHelper.getConn().createStatement();
        Set<Customer> relatedCustomers = new HashSet<>();
        ArrayList<Integer> orders = new ArrayList<>();
        // Select orderids from this customer
        ResultSet orderRs = stmt.executeQuery(String.format(
                "SELECT o_id FROM order_tab WHERE o_w_id = %d and o_d_id = %d and o_c_id = %d", cwid, cdid, cid));
        while (orderRs.next()) {
            int o_id = orderRs.getInt(1);
            orders.add(o_id);
        }
        if (orders.size() > 0) {
            for (int o_id : orders) {
                ResultSet relatedOrdersRs = stmt.executeQuery(String
                        .format("SELECT DISTINCT ol_w_id, ol_d_id, ol_o_id, COUNT(ol_number) FROM order_line_tab \n"
                                + "WHERE ol_i_id in ( SELECT DISTINCT ol_i_id FROM order_line_tab WHERE ol_w_id = %d and ol_d_id = %d and ol_o_id = %d )\n"
                                + "AND ol_w_id <> %d \n" + "GROUP BY (ol_w_id, ol_d_id,ol_o_id)\n"
                                + "HAVING COUNT(ol_number)>=2", cwid, cdid, o_id, cwid));
                ArrayList<Order> realtedOrders = new ArrayList<>();
                while (relatedOrdersRs.next()) {
                    int ol_w_id = relatedOrdersRs.getInt(1);
                    int ol_d_id = relatedOrdersRs.getInt(2);
                    int ol_o_id = relatedOrdersRs.getInt(3);
                    realtedOrders.add(new Order(ol_w_id, ol_d_id, ol_o_id));
                }
                relatedOrdersRs.close();

                ArrayList<String> conditions = new ArrayList<>();
                for (Order order : realtedOrders) {
                    conditions.add(String.format(" (o_w_id = %d AND o_d_id = %d AND o_id = %d) ", order.w_id,
                            order.d_id, order.o_id));
                }
                if (realtedOrders.size() > 0) {
                    ResultSet customerRs = stmt.executeQuery(
                            String.format("SELECT DISTINCT o_w_id, o_d_id, o_c_id FROM order_tab WHERE %s ;",
                                    String.join(" OR ", conditions)));
                    while (customerRs.next()) {
                        int c_w_id = customerRs.getInt(1);
                        int c_d_id = customerRs.getInt(2);
                        int c_id = customerRs.getInt(3);
                        relatedCustomers.add(new Customer(c_w_id, c_d_id, c_id));
                    }
                    customerRs.close();
                }
                relatedOrdersRs.close();
            }
        }
        orderRs.close();
        for (Customer cust : relatedCustomers) {
            System.out.printf("Related Customer: w_id:%d, d_id:%d, c_id: %d\n", cust.w_id, cust.d_id, cust.c_id);
        }

    }

    private static void getDbState() throws SQLException {
        System.out.println("======================DB State ============================");
        StringBuilder res = new StringBuilder();

        Statement warehouseSum = connHelper.getConn().createStatement();
        ResultSet warehouseResultSet = warehouseSum.executeQuery("SELECT sum(W_YTD) from warehouse_tab;");
        while (warehouseResultSet.next()) {
            BigDecimal w_ytd = warehouseResultSet.getBigDecimal(1);
            System.out.printf("W_YTD: %f\n", w_ytd);
            res.append(w_ytd.doubleValue() + "\n");
        }
        warehouseResultSet.close();

        Statement districtSum = connHelper.getConn().createStatement();
        ResultSet districtResultSet = districtSum
                .executeQuery("SELECT sum(D_YTD), sum(D_NEXT_O_ID) from district_tab;");
        while (districtResultSet.next()) {
            BigDecimal d_ytd = districtResultSet.getBigDecimal(1);
            BigDecimal d_next_o_ytd = districtResultSet.getBigDecimal(2);

            System.out.printf("D_YTD: %f\n", d_ytd);
            res.append(d_ytd.doubleValue() + "\n");
            System.out.printf("D_NEXT_O_ID: %f\n", d_next_o_ytd);
            res.append(d_next_o_ytd.doubleValue() + "\n");
        }
        districtResultSet.close();

        Statement customerSum = connHelper.getConn().createStatement();
        ResultSet customerResultSet = customerSum.executeQuery(
                "select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from Customer_tab;");
        while (customerResultSet.next()) {
            BigDecimal c_balance = customerResultSet.getBigDecimal(1);
            System.out.printf("C_BALANCE: %f\n", c_balance);
            res.append(c_balance.doubleValue() + "\n");
            BigDecimal c_tyd_payment = customerResultSet.getBigDecimal(2);
            System.out.printf("C_YTD_PAYMENT: %f\n", c_tyd_payment);
            res.append(c_tyd_payment.doubleValue() + "\n");
            BigDecimal c_payment_count = customerResultSet.getBigDecimal(3);
            System.out.printf("C_PAYMENT_CNT: %f\n", c_payment_count);
            res.append(c_payment_count.doubleValue() + "\n");
            BigDecimal c_delivery_cnt = customerResultSet.getBigDecimal(4);
            System.out.printf("C_DELIVERY_CNT: %f\n", c_delivery_cnt);
            res.append(c_delivery_cnt.doubleValue() + "\n");
        }
        customerResultSet.close();

        Statement orderSum = connHelper.getConn().createStatement();
        ResultSet orderResultSet = orderSum.executeQuery("select max(O_ID), sum(O_OL_CNT) from Order_tab;");
        while (orderResultSet.next()) {
            BigDecimal o_id = orderResultSet.getBigDecimal(1);
            BigDecimal o_ol_cnt = orderResultSet.getBigDecimal(2);

            System.out.printf("O_ID: %f\n", o_id);
            res.append(o_id.doubleValue() + "\n");
            System.out.printf("O_OL_CNT: %f\n", o_ol_cnt);
            res.append(o_ol_cnt.doubleValue() + "\n");
        }
        orderResultSet.close();

        Statement orderLineSum = connHelper.getConn().createStatement();
        ResultSet orderLineResultSet = orderLineSum
                .executeQuery("select sum(OL_AMOUNT), sum(OL_QUANTITY) from Order_Line_tab;");
        while (orderLineResultSet.next()) {
            BigDecimal ol_amount = orderLineResultSet.getBigDecimal(1);
            BigDecimal ol_quantity = orderLineResultSet.getBigDecimal(2);

            System.out.printf("OL_AMOUNT: %f\n", ol_amount);
            res.append(ol_amount.doubleValue() + "\n");
            System.out.printf("OL_QUANTITY: %f\n", ol_quantity);
            res.append(ol_quantity.doubleValue() + "\n");
        }
        orderLineResultSet.close();

        Statement stockSum = connHelper.getConn().createStatement();
        ResultSet stockResultSet = stockSum.executeQuery(
                "select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock_tab;");
        while (stockResultSet.next()) {
            BigDecimal s_quantity = stockResultSet.getBigDecimal(1);
            System.out.printf("S_QUANTITY: %f\n", s_quantity);
            res.append(s_quantity.doubleValue() + "\n");
            BigDecimal s_ytd = stockResultSet.getBigDecimal(2);
            System.out.printf("S_YTD: %f\n", s_ytd);
            res.append(s_ytd.doubleValue() + "\n");
            BigDecimal s_order_cnt = stockResultSet.getBigDecimal(3);
            System.out.printf("S_ORDER_CNT: %f\n", s_order_cnt);
            res.append(s_order_cnt.doubleValue() + "\n");
            BigDecimal s_remote_cnt = stockResultSet.getBigDecimal(4);
            System.out.printf("S_REMOTE_CNT: %f\n", s_remote_cnt);
            res.append(s_remote_cnt.doubleValue() + "\n");
        }
        stockResultSet.close();

        System.out.println("======================DB State Plain=======================");
        System.out.println(res);
        TransactionStatistics.printServerTime();
        System.out.println();
    }

}
