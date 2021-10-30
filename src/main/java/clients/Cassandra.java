package clients;

import clients.utils.TransactionStatistics;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.Scanner;

public class Cassandra {

    // Limit number of txns executed
    private static final int TXN_LIMIT = 200;

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("run the program by: ./Cassandra <host> <port> <schema_name> <client>\n e.g. ./Cassandra localhost 9042 A 1");
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
            System.err.println("run the program by: ./Cassandra <host> <port> <schema_name> <client>\n e.g. ./Cassandra localhost 9042 A 1");
            return;
        }

        System.out.printf("Running on host: %s:%d", host, port);
        System.out.println();

        CqlSession session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withKeyspace(schema_name)
//                .withLocalDatacenter("cs5424-c")
                .withLocalDatacenter("datacenter1")
                .build();

        FileInputStream stream = new FileInputStream(dataDir);
        Scanner scanner = new Scanner(stream);

        ArrayList<TransactionStatistics> latencies = new ArrayList<>();
        int txnCount = 0;
        while (scanner.hasNextLine() && txnCount < TXN_LIMIT) {
            txnCount++;
            String line = scanner.nextLine();
            String[] splits = line.split(",");
            char txnType = splits[0].toCharArray()[0];
            long latency = invokeTransaction(session, splits, scanner);
            latencies.add(new TransactionStatistics(txnType, latency));
            System.out.printf("<%d/20000> Tnx %c: %dms \n", txnCount, txnType, latency);
        }
        session.close();
        TransactionStatistics.printStatistics(latencies);
    }

    private static long invokeTransaction(CqlSession session, String[] splits, Scanner scanner) {
        long start = System.currentTimeMillis();
        switch (splits[0].toCharArray()[0]) {
            case 'N':
                int cid = Integer.parseInt(splits[1]);
                int wid = Integer.parseInt(splits[2]);
                int did = Integer.parseInt(splits[3]);
                int numItems = Integer.parseInt(splits[4]);
                ArrayList<Integer> items = new ArrayList<Integer>();
                ArrayList<Integer> supplierWarehouses = new ArrayList<Integer>();
                ArrayList<BigDecimal> quantities = new ArrayList<BigDecimal>();
                for (int i = 0; i < numItems; i++) {
                    String line = scanner.nextLine();
                    splits = line.split(",");
                    items.add(Integer.parseInt(splits[0]));
                    supplierWarehouses.add(Integer.parseInt(splits[1]));
                    quantities.add(BigDecimal.valueOf(Integer.parseInt(splits[2])));
                }
                newOrderTransaction(session, cid, wid, did, numItems, items, supplierWarehouses, quantities);
                break;

            case 'P':
                int cwid = Integer.parseInt(splits[1]);
                int cdid = Integer.parseInt(splits[2]);
                cid = Integer.parseInt(splits[3]);
                BigDecimal payment = new BigDecimal(splits[4]);
                paymentTransaction(session, cwid, cdid, cid, payment);
                break;

            case 'D':
                wid = Integer.parseInt(splits[1]);
                int carrierid = Integer.parseInt(splits[2]);
                deliveryTransaction(session, wid, carrierid);
                break;

            case 'O':
                cwid = Integer.parseInt(splits[1]);
                cdid = Integer.parseInt(splits[2]);
                cid = Integer.parseInt(splits[3]);
                orderStatusTransaction(session, cwid, cdid, cid);
                break;

            case 'S':
                wid = Integer.parseInt(splits[1]);
                did = Integer.parseInt(splits[2]);
                int t = Integer.parseInt(splits[3]);
                int l = Integer.parseInt(splits[4]);
                stockLevelTransaction(session, wid, did, t, l);
                break;

            case 'I':
                wid = Integer.parseInt(splits[1]);
                did = Integer.parseInt(splits[2]);
                l = Integer.parseInt(splits[3]);
                popularItemTransaction(session, wid, did, l);
                break;

            case 'T':
                topBalanceTransaction(session);
                break;

            case 'R':
                cwid = Integer.parseInt(splits[1]);
                cdid = Integer.parseInt(splits[2]);
                cid = Integer.parseInt(splits[3]);
                relatedCustomerTransaction(session, cwid, cdid, cid);
                break;
        }

        return System.currentTimeMillis() - start;
    }

    private static void newOrderTransaction(CqlSession session, int cid, int wid, int did, int number_of_items,
                                            ArrayList<Integer> items, ArrayList<Integer> supplier_warehouses, ArrayList<BigDecimal> quantities) {
        try {
            ResultSet rs = session.execute("SELECT * FROM district_tab WHERE D_W_ID = " + wid + " AND D_ID = " + did);
            Integer next_order_id = 0;
            next_order_id = rs.one().getInt("D_NEXT_O_ID");

            next_order_id += 1;

            PreparedStatement updateDistrict = session.prepare(
                    "UPDATE district_tab SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?");
            BoundStatement updateDistrictBound = updateDistrict.bind()
                    .setInt(0, next_order_id)
                    .setInt(1, wid)
                    .setInt(2, did);
            session.execute(updateDistrictBound);

            int all_local = 0;
            if (supplier_warehouses.stream().distinct().count() <= 1 && supplier_warehouses.contains(wid)) {
                all_local = 1;
            }

            PreparedStatement createOrder = session.prepare("INSERT INTO order_tab (O_W_ID, O_D_ID, O_ID, O_C_ID," +
                    " O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (?,?,?,?,?,?,?,?)");

            BoundStatement createOrderBound = createOrder.bind()
                    .setInt(0, wid)
                    .setInt(1, did)
                    .setInt(2, next_order_id)
                    .setInt(3, cid)
                    .setInt(4, 0)
                    .setBigDecimal(5, BigDecimal.valueOf(number_of_items))
                    .setBigDecimal(6, BigDecimal.valueOf(all_local))
                    .setDefaultTimestamp(System.currentTimeMillis());
            session.execute(createOrderBound);


            BigDecimal total_amount = new BigDecimal(0);
            ArrayList<String> itemNames = new ArrayList<String>();
            ArrayList<BigDecimal> itemPrices = new ArrayList<BigDecimal>();
            ArrayList<BigDecimal> itemStocks = new ArrayList<BigDecimal>();

            for (int idx = 0; idx < items.size(); idx++) {
                int current_item = items.get(idx);

                rs = session.execute("SELECT i_price, i_name FROM item_tab WHERE i_id = " + current_item);
                Row row = rs.one();
                BigDecimal price = row.getBigDecimal("i_price");
                String name = row.getString("i_name");

                itemNames.add(name);
                itemPrices.add(price);

                String strDid = "";
                if (did == 10) {
                    strDid = String.valueOf(did);
                } else {
                    strDid = "0" + did;
                }

                PreparedStatement itemStock = session.prepare("SELECT s_quantity,s_ytd,s_order_cnt," +
                        "s_remote_cnt, s_dist_" + strDid + " FROM stock_tab WHERE s_w_id=? AND s_i_id=?;");
                BoundStatement itemStockBound = itemStock.bind()
                        .setInt(0, wid)
                        .setInt(1, current_item);
                rs = session.execute(itemStockBound);
                row = rs.one();
                BigDecimal s_quantity = row.getBigDecimal("s_quantity");
                BigDecimal s_ytd = row.getBigDecimal("s_ytd");
                int s_order_cnt = row.getInt("s_order_cnt");
                int s_remote_cnt = row.getInt("s_remote_cnt");

                BigDecimal adjusted_quantity = s_quantity.subtract(quantities.get(idx));
                if (adjusted_quantity.compareTo(BigDecimal.TEN) == -1) {
                    adjusted_quantity.add(BigDecimal.valueOf(100));
                }
                if (supplier_warehouses.get(idx) != wid) {
                    s_remote_cnt += 1;
                }

                PreparedStatement updateStock = session.prepare("UPDATE stock_tab SET " +
                        "s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE s_w_id=? AND s_i_id=?;");
                BoundStatement updateStockBound = updateStock.bind()
                        .setBigDecimal(0, adjusted_quantity)
                        .setBigDecimal(1, s_ytd.add(quantities.get(idx)))
                        .setInt(2, s_order_cnt + 1)
                        .setInt(3, s_remote_cnt)
                        .setInt(4, wid)
                        .setInt(5, current_item);

                session.execute(updateStockBound);

                itemStocks.add(adjusted_quantity);

                BigDecimal item_amount = price.multiply(quantities.get(idx));
                total_amount = total_amount.add(item_amount);

                PreparedStatement insertOrderLine = session.prepare("INSERT INTO order_line_tab (OL_W_ID, OL_D_ID, " +
                        "OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
                BoundStatement insertOrderLineBound = insertOrderLine.bind()
                        .setInt(0, wid)
                        .setInt(1, did)
                        .setInt(2, next_order_id)
                        .setInt(3, idx)
                        .setInt(4, current_item)
                        .setDefaultTimestamp( 0)
                        .setBigDecimal(6, item_amount)
                        .setInt(7, supplier_warehouses.get(idx))
                        .setBigDecimal(8, quantities.get(idx))
                        .setString(9, "S_DIST_" + did);
                session.execute(insertOrderLineBound);
            }

            rs = session.execute("SELECT c_last, c_credit, c_discount FROM customer_tab " +
                    "WHERE c_id = " + cid + " AND c_d_id = " + did + " AND c_w_id = " + wid);
            Row row = rs.one();
            BigDecimal discount = row.getBigDecimal("c_discount");
            String last_name = row.getString("c_last");
            String credit = row.getString("c_credit");

            rs = session.execute("SELECT w_tax FROM warehouse_tab WHERE w_id = " + wid);
            BigDecimal warehouse_tax_rate = rs.one().getBigDecimal("w_tax");

            rs = session.execute("SELECT d_tax FROM district_tab WHERE d_id = " + did + " AND d_w_id = " + wid);
            BigDecimal district_tax_rate = rs.one().getBigDecimal("d_tax");

            total_amount = total_amount.multiply((warehouse_tax_rate.add(BigDecimal.valueOf(1)).
                    add(district_tax_rate)).multiply(BigDecimal.valueOf(1)).subtract(discount));

            System.out.printf("============================ New Order Transactions ============================ \n");
            System.out.printf("WarehouseID %d, DistrictID: %d, CustomerID: %d \n", wid, did, cid);
            System.out.printf("LastName %s, Credit: %s, Discount: %s \n", last_name, credit, discount);
            System.out.printf("WarehouseTaxRate %s, DistrictTaxRate: %s \n", warehouse_tax_rate, district_tax_rate);
            System.out.printf("OrderID %s, OrderEntryDate: %s, NumberOfItems \n", next_order_id, "", number_of_items);
            System.out.printf("TotalAmount %s \n", total_amount);
            for (int idx = 0; idx < items.size(); idx++) {
                System.out.printf(" ItemID %s, SupplierWarehouse %s, Quantity %s \n", items.get(idx), supplier_warehouses.get(idx), quantities.get(idx));
                System.out.printf(" ItemName %s, ItemAmount %s, StockQuantity %s \n", itemNames.get(idx),
                        itemPrices.get(idx).multiply(quantities.get(idx)), itemStocks.get(idx));
            }
            System.out.printf("=============================================================================== \n");
            System.out.println();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Create Order Error! cause = [%s] message = [%s]", e.getCause(),
                    e.getMessage());
        }
    }

    private static void paymentTransaction(CqlSession session, int cwid, int cdid, int cid, BigDecimal payment) {
        try {
            ResultSet warehouse_result = session.execute(
                    "SELECT w_ytd FROM warehouse_tab WHERE w_id = " + cwid + " ;");
            Row warehouseRow = warehouse_result.one();
            BigDecimal old_ytd = warehouseRow.getBigDecimal("w_ytd");
            PreparedStatement updateWarehouse = session.prepare(
                    "UPDATE warehouse_tab SET W_YTD = ? WHERE W_ID = ?;");
            BoundStatement updateWarehouseBound = updateWarehouse.bind()
                    .setBigDecimal(0, old_ytd.add(payment))
                    .setInt(1, cwid);
            session.execute(updateWarehouseBound);

            ResultSet customer_result = session.execute(
                    "SELECT c_balance, c_ytd_payment, c_payment_cnt FROM customer_tab " +
                            String.format("WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d ;", cwid, cdid, cid));
            Row customer_row = customer_result.one();
            BigDecimal old_c_balance = customer_row.getBigDecimal("c_balance");
            float old_c_ytd_payment = customer_row.getFloat("c_ytd_payment");
            int old_c_payment_cnt = customer_row.getInt("c_payment_cnt");

            PreparedStatement updateCustomer = session.prepare(
                    "Update customer_tab " +
                            "SET c_balance = ?,c_ytd_payment = ?, c_payment_cnt = ? " +
                            "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? ;");
            BoundStatement updateCustomerBound = updateCustomer.bind()
                    .setBigDecimal(0, old_c_balance.subtract(payment))
                    .setFloat(1, old_c_ytd_payment + payment.floatValue())
                    .setInt(2, old_c_payment_cnt + 1)
                    .setInt(3, cwid)
                    .setInt(4, cdid)
                    .setInt(5, cid);
            session.execute(updateCustomerBound);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void deliveryTransaction(CqlSession session, int wid, int carrierid) {

    }

    private static void orderStatusTransaction(CqlSession session, int cwid, int cdid, int cid) {

    }

    private static void stockLevelTransaction(CqlSession session, int wid, int did, int threshold, int l) {
        try {
            ResultSet rs = session.execute("SELECT d_next_o_id FROM district_tab WHERE d_w_id=" + wid + " AND d_id=" + did + ";");
            Integer latest_order_id = rs.one().getInt("d_next_o_id");

            Integer earliest_order_id = latest_order_id - l;
            PreparedStatement getOrderLine = session.prepare("SELECT ol_i_id FROM order_line_tab " +
                    "WHERE ol_d_id=? AND ol_w_id=? AND ol_o_id>? AND ol_o_id<?;");
            BoundStatement getOrderLineBound = getOrderLine.bind()
                    .setInt(0, did)
                    .setInt(1, wid)
                    .setInt(2, earliest_order_id)
                    .setInt(3, latest_order_id);
            rs = session.execute(getOrderLineBound);

            int low_stock_count = 0;
            Iterator<Row> iterator = rs.iterator();
            while (iterator.hasNext()) {
                Row curr_row = iterator.next();
                int item = curr_row.getInt("ol_i_id");
                ResultSet curr_quantity = session.execute("SELECT s_quantity FROM stock_tab WHERE s_w_id=" + wid + " AND s_i_id=" + item);
                BigDecimal quantity = curr_quantity.one().getBigDecimal("s_quantity");
                if (quantity.compareTo(BigDecimal.valueOf(threshold)) == -1) {
                    low_stock_count += 1;
                }
            }

            System.out.printf("============================ Stock Level Transaction ============================ \n");
            System.out.printf("Searching at Warehouse %d, District %d, Threshold %d, Last %d items \n", wid, did, threshold, l);
            System.out.printf("Total number of low stock level items is %d \n", low_stock_count);
            System.out.printf("================================================================================= \n");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Stock Level Transaction Error! cause = [%s] message = [%s]", e.getCause(), e.getMessage());
        }
    }

    private static void popularItemTransaction(CqlSession session, int wid, int did, int l) {

    }

    private static void topBalanceTransaction(CqlSession session) {

    }

    private static void relatedCustomerTransaction(CqlSession session, int cwid, int cdid, int cid) {

    }
}
