package clients;

import clients.utils.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Cassandra {

    // Limit number of txns executed
    private static int TXN_LIMIT = 2000;
    private static final ConsistencyLevel USE_QUORUM = ConsistencyLevel.QUORUM;


    // For testing in local only:
//    private static final ConsistencyLevel USE_QUORUM = ConsistencyLevel.ONE;



    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("run the program by: ./Cassandra <host> <port> <schema_name> <client> <csv> <db_state>\n " +
                    "e.g. ./Cassandra localhost 9042 A 1 out/cassandra-A-local.csv 0");
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
            System.err.println("run the program by: ./Cassandra <host> <port> <schema_name> <client>\n " +
                    "e.g. ./Cassandra localhost 9042 A 1 out/cassandra-A-local.csv 0");
            return;
        }

        DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                        .startProfile("slow")
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                        .endProfile()
                        .build();

        System.out.printf("Running on host: %s:%d", host, port);
        System.out.println();

        CqlSession session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withConfigLoader(loader)
                .withKeyspace(schema_name)
                .withLocalDatacenter("datacenter1")


                .build();

        FileInputStream stream = new FileInputStream(dataDir);
        Scanner scanner = new Scanner(stream);

        // Client 1 always write CSV header
        if (client.equals("1")) {
            TransactionStatistics.writeCsvHeader(csvPath);
        }

        ArrayList<TransactionStatistics> latencies = new ArrayList<>();
        int txnCount = 0;
        long clientStartTime = System.currentTimeMillis();
        StringBuilder errors = new StringBuilder();
        while (scanner.hasNextLine() && txnCount < TXN_LIMIT) {
            txnCount++;
            String line = scanner.nextLine();
            String[] splits = line.split(",");
            char txnType = splits[0].toCharArray()[0];
            try {
                float latency = invokeTransaction(session, splits, scanner);
                // TODO: Add retry count
                latencies.add(new TransactionStatistics(txnType, latency / 1000000, 0));
                System.out.printf("<%d/20000> Tnx %c: %.2fms, retry: %d times \n", txnCount, txnType, latency / 1000000, 0);
            } catch (Exception e) {
                errors.append("Error at txn " + txnCount + "type " + txnType + '\n');
                errors.append(e.getLocalizedMessage() + '\n');
//                e.printStackTrace();
            }
        }
        if (isDbState == 1) {
            try {
                getDbState(session);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("DB State Error, Other clients still runing... Ignore...");
            }
        }
        session.close();
        float clientTotalTime = (float) (System.currentTimeMillis() - clientStartTime) / 1000;
        System.out.println("Errors: (If any)");
        System.out.println(errors);
        String message = TransactionStatistics.getStatistics(latencies, clientTotalTime, client, csvPath);

        System.err.println(message);
    }

    private static long invokeTransaction(CqlSession session, String[] splits, Scanner scanner) {
        long start = System.nanoTime();
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

        return System.nanoTime() - start;
    }

    private static void newOrderTransaction(CqlSession session, int cid, int wid, int did, int number_of_items,
                                            ArrayList<Integer> items, ArrayList<Integer> supplier_warehouses, ArrayList<BigDecimal> quantities) {
        try {
            ResultSet rs = session.execute(session.prepare("SELECT D_NEXT_O_ID FROM district_tab WHERE D_W_ID = " + wid + " AND D_ID = " + did).bind().setConsistencyLevel(USE_QUORUM));
            Integer next_order_id = 0;
            next_order_id = rs.one().getInt("D_NEXT_O_ID");

            next_order_id += 1;

            PreparedStatement updateDistrict = session.prepare(
                    "UPDATE district_tab SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?");
            BoundStatement updateDistrictBound = updateDistrict.bind()
                    .setInt(0, next_order_id)
                    .setInt(1, wid)
                    .setInt(2, did)
                    .setConsistencyLevel(USE_QUORUM);

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
//                    .setInt(4, 0)
                    .setString(4, "null")
                    .setBigDecimal(5, BigDecimal.valueOf(number_of_items))
                    .setBigDecimal(6, BigDecimal.valueOf(all_local))
                    .setDefaultTimestamp(System.currentTimeMillis())
                    .setConsistencyLevel(USE_QUORUM);
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
                        .setInt(1, current_item).setConsistencyLevel(USE_QUORUM);
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
                        .setInt(5, current_item)
                        .setConsistencyLevel(USE_QUORUM);

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
                        .setDefaultTimestamp(0)
                        .setBigDecimal(6, item_amount)
                        .setInt(7, supplier_warehouses.get(idx))
                        .setBigDecimal(8, quantities.get(idx))
                        .setString(9, "S_DIST_" + did)
                        .setConsistencyLevel(USE_QUORUM);
                session.execute(insertOrderLineBound);
            }

            rs = session.execute(session.prepare("SELECT c_last, c_credit, c_discount FROM customer_tab " +
                    "WHERE c_id = " + cid + " AND c_d_id = " + did + " AND c_w_id = " + wid).bind().setConsistencyLevel(USE_QUORUM));
            Row row = rs.one();
            BigDecimal discount = row.getBigDecimal("c_discount");
            String last_name = row.getString("c_last");
            String credit = row.getString("c_credit");

            rs = session.execute(session.prepare("SELECT w_tax FROM warehouse_tab WHERE w_id = " + wid).bind().setConsistencyLevel(USE_QUORUM));
            BigDecimal warehouse_tax_rate = rs.one().getBigDecimal("w_tax");

            rs = session.execute(session.prepare("SELECT d_tax FROM district_tab WHERE d_id = " + did + " AND d_w_id = " + wid).bind().setConsistencyLevel(USE_QUORUM));
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
            Row warehouseRow = session.execute(
                    session.prepare(" SELECT w_ytd FROM warehouse_tab WHERE w_id= " + cwid + " ;")
                            .bind()
                            .setConsistencyLevel(USE_QUORUM)).one();
            BigDecimal old_ytd = warehouseRow.getBigDecimal("w_ytd");

            session.execute(session.prepare(
                            "UPDATE warehouse_tab SET W_YTD = ? WHERE W_ID = ?;").bind()
                    .setBigDecimal(0, old_ytd.add(payment))
                    .setInt(1, cwid).setConsistencyLevel(USE_QUORUM));

            Row customer_row = session.execute(session.prepare(
                            "SELECT c_balance, c_ytd_payment, c_payment_cnt FROM customer_tab " +
                                    String.format("WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d  ALLOW FILTERING; ", cwid, cdid, cid))
                    .bind()
                    .setConsistencyLevel(USE_QUORUM)).one();

            BigDecimal old_c_balance = customer_row.getBigDecimal("c_balance");
            float old_c_ytd_payment = customer_row.getFloat("c_ytd_payment");
            int old_c_payment_cnt = customer_row.getInt("c_payment_cnt");

            session.execute(session.prepare(
                            "Update customer_tab " +
                                    "SET c_balance = ?,c_ytd_payment = ?, c_payment_cnt = ? " +
                                    "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? ;").bind()
                    .setBigDecimal(0, old_c_balance.subtract(payment))
                    .setFloat(1, old_c_ytd_payment + payment.floatValue())
                    .setInt(2, old_c_payment_cnt + 1)
                    .setInt(3, cwid)
                    .setInt(4, cdid)
                    .setInt(5, cid)
                    .setConsistencyLevel(USE_QUORUM));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void deliveryTransactionUnit(CqlSession session, int wid, int carrierid, int did) {
        // get the yet-to-deliver order with its client id
        PreparedStatement getOrderAndCustomerId = session.prepare(
                "SELECT\n" +
                        "\to_id,\n" +
                        "\to_c_id\n" +
                        "FROM\n" +
                        "\torder_tab\n" +
                        "WHERE\n" +
                        "\to_w_id = ?\n" +
                        "\tAND o_d_id = ?\n" +
                        "\tAND o_carrier_id = 'null'\n" +
                        "ORDER BY\n" +
                        "\to_id\n" +
                        "LIMIT 1 ALLOW FILTERING"
        );
        BoundStatement getOrderAndCustomerIdBound = getOrderAndCustomerId.bind()
                .setInt(0, wid)
                .setInt(1, did)
                .setConsistencyLevel(USE_QUORUM);
        ResultSet rs = session.execute(getOrderAndCustomerIdBound);

        // proceed when a yet-to-deliver order exists
        if (rs.iterator().hasNext()) {
            Row row = rs.one();
            int orderID = row.getInt("o_id");
            int customerID = row.getInt("o_c_id");

//            System.out.printf("order: %d customer:%d ", orderID, customerID);

            // assign the carrier id to the order
            PreparedStatement updateOrder = session.prepare(
                    "UPDATE\n" +
                            "\torder_tab\n" +
                            "SET\n" +
                            "\to_carrier_id = ?\n" +
                            "WHERE\n" +
                            "\to_w_id = ?\n" +
                            "\tAND o_d_id = ?\n" +
                            "\tAND o_id = ?"
            );
            BoundStatement updateOrderBound = updateOrder.bind()
                    .setString(0, String.valueOf(carrierid))
                    .setInt(1, wid)
                    .setInt(2, did)
                    .setInt(3, orderID)
                    .setConsistencyLevel(USE_QUORUM);
            session.execute(updateOrderBound);

            // assign the current timestamp to each order line
            // get the count and amount sum of order lines
            PreparedStatement getOrderLineCount = session.prepare(
                    "SELECT\n" +
                            "\tcount(1), sum(ol_amount) as sum\n" +
                            "FROM\n" +
                            "\torder_line_tab\n" +
                            "WHERE\n" +
                            "\tol_w_id = ?\n" +
                            "\tAND ol_d_id = ?\n" +
                            "\tAND ol_o_id = ?"
            );
            BoundStatement getOrderLineCountBound = getOrderLineCount.bind()
                    .setInt(0, wid)
                    .setInt(1, did)
                    .setInt(2, orderID)
                    .setConsistencyLevel(USE_QUORUM);
            rs = session.execute(getOrderLineCountBound);
            row = rs.one();
            long orderLineCount = row.getLong("count");
            BigDecimal orderLineSum = row.getBigDecimal("sum");
//            System.out.printf("order count: %d order sum: %d", orderLineCount, orderLineSum.intValue());

            // update per order line
            PreparedStatement updateOrderLine = session.prepare(
                    "UPDATE\n" +
                            "\torder_line_tab\n" +
                            "SET\n" +
                            "\tol_delivery_d = toTimestamp (now())\n" +
                            "WHERE\n" +
                            "\tol_w_id = ?\n" +
                            "\tAND ol_d_id = ?\n" +
                            "\tAND ol_o_id = ?\n" +
                            "\tAND ol_number = ?;"
            );
            for (int line = 1; line <= orderLineCount; line++) {
                BoundStatement updateOrderLineBound = updateOrderLine.bind()
                        .setInt(0, wid)
                        .setInt(1, did)
                        .setInt(2, orderID)
                        .setInt(3, line)
                        .setConsistencyLevel(USE_QUORUM);
                session.execute(updateOrderLineBound);
            }

            // update the customer's balance and delivery info
            // get the present customer balance and delivery count
            PreparedStatement getCustomerInfo = session.prepare(
                    "SELECT\n" +
                            "\tc_balance,\n" +
                            "\tc_delivery_cnt\n" +
                            "FROM\n" +
                            "\tcustomer_tab\n" +
                            "WHERE\n" +
                            "\tc_id = ?\n" +
                            "\tAND c_w_id = ?\n" +
                            "\tAND c_d_id = ?"
            );
            BoundStatement getCustomerInfoBound = getCustomerInfo.bind()
                    .setInt(0, customerID)
                    .setInt(1, wid)
                    .setInt(2, did)
                    .setConsistencyLevel(USE_QUORUM);
            rs = session.execute(getCustomerInfoBound);
            row = rs.one();
            BigDecimal balance = row.getBigDecimal("c_balance");
            int count = row.getInt("c_delivery_cnt");
//                System.out.printf("balance%f count%d", balance, count);

            // update the present customer balance and delivery count
            PreparedStatement updateCustomerInfo = session.prepare(
                    "UPDATE\n" +
                            "\tcustomer_tab\n" +
                            "SET\n" +
                            "\tc_balance = ?,\n" +
                            "\tc_delivery_cnt = ?\n" +
                            "WHERE\n" +
                            "\tc_id = ?\n" +
                            "\tAND c_w_id = ?\n" +
                            "\tAND c_d_id = ?"
            );
            BoundStatement updateCustomerInfoBound = updateCustomerInfo.bind()
                    .setBigDecimal(0, balance.add(orderLineSum))
                    .setInt(1, count + 1)
                    .setInt(2, customerID)
                    .setInt(3, wid)
                    .setInt(4, did)
                    .setConsistencyLevel(USE_QUORUM);
            session.execute(updateCustomerInfoBound);
//            System.out.printf("did: %d order: %d customer %d new balance: %f new count : %d\n", did, orderID, customerID, balance.add(orderLineSum), count + 1);
        }
    }

    private static void deliveryTransaction(CqlSession session, int wid, int carrierid) {
        System.out.println("Delivery Txn");

        // parallel version
        List<Integer> didRange = IntStream.rangeClosed(1, 10)
                .boxed().collect(Collectors.toList());
        didRange.parallelStream().forEach(
                did -> {
//                    System.out.printf("did: %d ", did);
                    deliveryTransactionUnit(session, wid, carrierid, did);
                }
        );

        // serial version
//        for(int did = 1; did <= 10; did ++) {
//            System.out.printf("did: %d ", did);
//            deliveryTransactionUnit(session, wid, carrierid, did);
//        }
    }

    private static void orderStatusTransaction(CqlSession session, int cwid, int cdid, int cid) {
        String get_customer = "select c_first, c_middle, c_last, c_balance from customer_tab "
                + "where c_w_id = %d and c_d_id = %d and c_id = %d ";
        String get_last_order = "SELECT o_w_id, o_d_id, o_c_id, o_id, o_entry_d, o_carrier_id "
                + "FROM order_tab "
                + "WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d order by o_id desc LIMIT 1 ALLOW FILTERING";
        String get_order_items = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
                + "from order_line_tab where ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d ";

        // customer last order
        Row customer = session.execute(session.prepare(String.format(get_customer, cwid, cdid, cid)).bind().setConsistencyLevel(ConsistencyLevel.ONE)).one();
        Row last_order = session.execute(session.prepare(String.format(get_last_order, cwid, cdid, cid)).bind().setConsistencyLevel(ConsistencyLevel.ONE)).one();
        int last_order_id = last_order.getInt("o_id");

        System.out.printf("Customer name: %s %s %s, Balance: %f\n",
                customer.getString("c_first"),
                customer.getString("c_middle"),
                customer.getString("c_last"),
                customer.getBigDecimal("c_balance").doubleValue());
        System.out.printf("Customer last order id: %d, Entry Datetime: %s, Carrier id: %s\n",
                last_order_id,
                last_order.getInstant("o_entry_d").toString(),
                last_order.getString("o_carrier_id"));

        // order items
        ResultSet rs_order_items = session.execute(session.prepare(String.format(get_order_items, cwid, cdid, last_order_id)).bind().setConsistencyLevel(ConsistencyLevel.ONE));
        for (Row item : rs_order_items) {
            System.out.printf("Item id: %d, Warehouse id: %d, Quantity: %f, Price: %f, Delivery Datetime: %s\n",
                    item.getInt("ol_i_id"),
                    item.getInt("ol_supply_w_id"),
                    item.getBigDecimal("ol_quantity").doubleValue(),
                    item.getBigDecimal("ol_amount").doubleValue(),
                    item.getInstant("ol_delivery_d") != null ? item.getInstant("ol_delivery_d").toString() : "");
        }


    }

    private static void stockLevelTransaction(CqlSession session, int wid, int did, int threshold, int l) {
        try {
            ResultSet rs = session.execute(session.prepare("SELECT d_next_o_id FROM district_tab WHERE d_w_id=" + wid + " AND d_id=" + did + ";").bind().setConsistencyLevel(ConsistencyLevel.ONE));
            Integer latest_order_id = rs.one().getInt("d_next_o_id");

            Integer earliest_order_id = latest_order_id - l;
            PreparedStatement getOrderLine = session.prepare("SELECT ol_i_id FROM order_line_tab " +
                    "WHERE ol_d_id=? AND ol_w_id=? AND ol_o_id>? AND ol_o_id<?;");
            BoundStatement getOrderLineBound = getOrderLine.bind()
                    .setInt(0, did)
                    .setInt(1, wid)
                    .setInt(2, earliest_order_id)
                    .setInt(3, latest_order_id)
                    .setConsistencyLevel(ConsistencyLevel.ONE);
            rs = session.execute(getOrderLineBound);

            int low_stock_count = 0;
            Iterator<Row> iterator = rs.iterator();
            while (iterator.hasNext()) {
                Row curr_row = iterator.next();
                int item = curr_row.getInt("ol_i_id");
                ResultSet curr_quantity = session.execute(session.prepare("SELECT s_quantity FROM stock_tab WHERE s_w_id=" + wid + " AND s_i_id=" + item).bind().setConsistencyLevel(ConsistencyLevel.ONE));
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
        String get_N = "select d_next_o_id from district_tab where d_w_id = %d and d_id = %d";
        String get_order = "select o_id, o_entry_d, o_c_id from order_tab where o_w_id = %d and o_d_id = %d and o_id in ( %s )";
        String get_order_lines = "select ol_o_id, ol_i_id, ol_quantity from order_line_tab "
                + "where ol_w_id = %d and ol_d_id = %d and ol_o_id in ( %s )";
        String get_customer = "select c_id, c_first, c_middle, c_last from customer_tab "
                + "where c_w_id = %d and c_d_id = %d and c_id in ( %s )";
        String get_items = "select i_id, i_name from item_tab where i_id in ( %s )";


        Integer N = session.execute(session.prepare(String.format(get_N, wid, did)).bind().setConsistencyLevel(ConsistencyLevel.ONE)).one().getInt("d_next_o_id");


        StringJoiner o_ids = new StringJoiner(",");
        for (int i = N - l; i < N; i++) {
            o_ids.add((N - i) + "");
        }

        List<Row> orders = session.execute(session.prepare(String.format(get_order, wid, did, o_ids.toString())).bind().setConsistencyLevel(ConsistencyLevel.ONE)).all();
        List<Row> order_lines = session.execute(session.prepare(String.format(get_order_lines, wid, did, o_ids.toString())).bind().setConsistencyLevel(ConsistencyLevel.ONE)).all();

        StringJoiner c_ids = new StringJoiner(",");
        for (Row order : orders) {
            c_ids.add(order.getInt("o_c_id") + "");
        }

        List<Row> customers = session.execute(session.prepare(String.format(get_customer, wid, did, c_ids.toString())).bind().setConsistencyLevel(ConsistencyLevel.ONE)).all();

        System.out.printf("WID: %d, DID: %d, Number of last orders: %d\n", wid, did, l);
        System.out.println();


        Set<Integer> all_popular_items = new HashSet<>();
        List<Set<Integer>> popular_items_each_order = new ArrayList<Set<Integer>>();
        Map<Integer, Row> customer_map = new HashMap<>();
        Map<Integer, List<Row>> ol_map = new HashMap<>();
        Map<Integer, String> item_map = new HashMap<>();

        // group order_line by o_id
        for (Row ol : order_lines) {
            Integer ol_o_id = ol.getInt("ol_o_id");
            if (!ol_map.containsKey(ol_o_id)) {
                List<Row> ol_list = new ArrayList<Row>();
                ol_list.add(ol);
                ol_map.put(ol_o_id, ol_list);
            } else {
                List<Row> ol_list = ol_map.get(ol_o_id);
                ol_list.add(ol);
                ol_map.put(ol_o_id, ol_list);
            }
        }
        // group customers by c_id
        for (Row customer : customers) {
            customer_map.put(customer.getInt("c_id"), customer);
        }

        for (Row order : orders) {
            int o_id = order.getInt("o_id");
            Row customer = customer_map.get(order.getInt("o_c_id"));
            System.out.printf(String.format("OID: %d, O_ENTRY_D: %s, Customer Name: %s\n",
                    o_id,
                    order.getInstant("o_entry_d").toString(),
                    customer.getString("c_first") + " " + customer.getString("c_middle") + " " + customer.getString("c_last")));

            List<Row> ols = ol_map.get(o_id);
            Map<Integer, Double> quantity_map = new HashMap<>();
            for (Row ol : ols) {
                Integer i_id = ol.getInt("ol_i_id");
                double ol_quantity = ol.getBigDecimal("ol_quantity").doubleValue();
                if (!quantity_map.containsKey(i_id)) {
                    quantity_map.put(i_id, ol_quantity);
                } else {
                    quantity_map.put(i_id, quantity_map.get(i_id) + ol_quantity);
                }
            }
            Double max_quantity = Double.MIN_VALUE;
            for (Map.Entry<Integer, Double> q : quantity_map.entrySet()) {
                max_quantity = Math.max(max_quantity, q.getValue());
            }

            Set<Integer> i_ids = new HashSet<>();
            for (Row ol : ols) {
                if (ol.getBigDecimal("ol_quantity").doubleValue() == max_quantity) {
                    all_popular_items.add(ol.getInt("ol_i_id"));
                    i_ids.add(ol.getInt("ol_i_id"));
                }
            }

            StringJoiner i_ids_str = new StringJoiner(",");
            for (Integer i_id : i_ids) {
                i_ids_str.add(i_id + "");
            }

            List<Row> items = session.execute(String.format(get_items, i_ids_str.toString())).all();
            for (Row item : items) {
                item_map.put(item.getInt("i_id"), item.getString("i_name"));
                System.out.printf("Popular I_NAME: %s, Quantity: %f\n",
                        item.getString("i_name"),
                        max_quantity);
            }

            popular_items_each_order.add(i_ids);

        }

        for (Integer i_id : all_popular_items) {
            int count = 0;
            for (Set<Integer> items : popular_items_each_order) {
                if (items.contains(i_id)) {
                    count++;
                }
            }
            System.out.printf("Popular I_NAME: %s, Percentage of Orders having Popular Items: %f\n",
                    item_map.get(i_id),
                    (float) count * 1 / orders.size());
        }
    }

    private static void topBalanceTransaction(CqlSession session) {
        System.out.println("Top Balance Txn");

        CustomerBalanceComparator comparator = new CustomerBalanceComparator();
        PriorityQueue<CustomerBalance> customersWithBalance = new PriorityQueue<>(10, comparator);

        // get the 10 customers with biggest
        for (int wid = 1; wid <= 10; wid++) {
            PreparedStatement getCustomerBalanceInfo = session.prepare(
                    "SELECT\n" +
                            "\tc_w_id,\n" +
                            "\tc_d_id,\n" +
                            "\tc_first,\n" +
                            "\tc_middle,\n" +
                            "\tc_last,\n" +
                            "\tc_balance\n" +
                            "FROM\n" +
                            "\tcustomer_tab_by_balance\n" +
                            "WHERE\n" +
                            "\tc_w_id = ?\n" +
                            "ORDER BY\n" +
                            "\tc_balance DESC\n" +
                            "LIMIT 10"
            );
            BoundStatement getCustomerBalanceInfoBound = getCustomerBalanceInfo.bind()
                    .setInt(0, wid).setConsistencyLevel(ConsistencyLevel.ONE);

            ResultSet rs = session.execute(getCustomerBalanceInfoBound);

            while (rs.iterator().hasNext()) {
                Row row = rs.one();
                CustomerBalance customer = new CustomerBalance(
                        row.getInt("c_w_id"),
                        row.getInt("c_d_id"),
                        row.getString("c_first"),
                        row.getString("c_middle"),
                        row.getString("c_last"),
                        row.getBigDecimal("c_balance")
                );
                customersWithBalance.add(customer);
            }
        }

        // compose results based on the top 10 balanced customers
        for (int i = 0; i < 10; i++) {
            CustomerBalance customer = customersWithBalance.poll();

            //get warehouse name
            PreparedStatement getWarehouseName = session.prepare(
                    "SELECT\n" +
                            "\tw_name\n" +
                            "FROM\n" +
                            "\twarehouse_tab\n" +
                            "WHERE\n" +
                            "\tw_id = ?"
            );
            BoundStatement getWarehouseNameBound = getWarehouseName.bind()
                    .setInt(0, customer.wid)
                    .setConsistencyLevel(ConsistencyLevel.ONE);
            ResultSet rs = session.execute(getWarehouseNameBound);
            String warehouseName = rs.one().getString("w_name");

            // get district name
            PreparedStatement getDistrictName = session.prepare(
                    "SELECT\n" +
                            "\td_name\n" +
                            "FROM\n" +
                            "\tdistrict_tab\n" +
                            "WHERE\n" +
                            "\td_id = ?\n" +
                            "\tAND d_w_id = ?"
            );
            BoundStatement getDistrictNameBound = getDistrictName.bind()
                    .setInt(0, customer.did)
                    .setInt(1, customer.wid)
                    .setConsistencyLevel(ConsistencyLevel.ONE);
            rs = session.execute(getDistrictNameBound);
            String districtName = rs.one().getString("d_name");

            System.out.printf("Customer Name: %-36s\tBalance: %-12.2f\tWarehouse Name: %-10s\tDistrict Name: %-10s\n",
                    customer.name_first + ' ' + customer.name_middle + ' ' + customer.name_last,
                    customer.balance, warehouseName, districtName);
        }
    }

    private static void relatedCustomerTransaction(CqlSession session, int cwid, int cdid, int cid) {

        Set<Customer> relatedCustomerSet = new HashSet<>();

        ResultSet rs;
        // get related orders
        PreparedStatement getOrder = session.prepare(
                "SELECT o_id FROM order_tab WHERE o_w_id = ? and o_d_id = ? and o_c_id = ? " +
                        "ALLOW FILTERING "
        );
        BoundStatement getOrdersBound = getOrder.bind()
                .setInt(0, cwid)
                .setInt(1, cdid)
                .setInt(2, cid)
                .setConsistencyLevel(ConsistencyLevel.ONE);
        rs = session.execute(getOrdersBound);
        List<Row> orders = rs.all();
        ArrayList<Set<Integer>> itemSets = new ArrayList<>();
        Set<Order> relatedOrders = new HashSet<>();
        for (Row order : orders) {

            rs = session.execute(session.prepare(
                            "SELECT ol_i_id FROM order_line_tab WHERE ol_w_id = ? and ol_d_id = ? and ol_o_id = ? " +
                                    "ALLOW FILTERING ")
                    .bind()
                    .setInt(0, cwid)
                    .setInt(1, cdid)
                    .setInt(2, order.getInt("o_id"))
                    .setConsistencyLevel(ConsistencyLevel.ONE));
            List<Row> items = rs.all();

            Set<Order> orderedAtLeastOnce = new HashSet<>();

            for (Row item : items) {
                ResultSet relatedOrdersForItem = session.execute(session.prepare(String.format("SELECT ol_w_id, ol_d_id, ol_o_id FROM order_line_tab \n" +
                                "WHERE ol_i_id = %d \n" +
                                "ALLOW FILTERING ", item.getInt("ol_i_id")))
                        .bind()
                        .setConsistencyLevel(ConsistencyLevel.ONE));

                for (Row relatedOrderForItem : relatedOrdersForItem) {
                    Order newOrder = new Order(relatedOrderForItem.getInt("ol_w_id"),
                            relatedOrderForItem.getInt("ol_d_id"),
                            relatedOrderForItem.getInt("ol_o_id"));
                    if (newOrder.w_id != cwid) {
                        if (orderedAtLeastOnce.contains(newOrder)) {
                            relatedOrders.add(newOrder);
                        }
                        orderedAtLeastOnce.add(newOrder);
                    }
                }
            }


            for (Order relatedOrder : relatedOrders) {
                Row relatedCustomer = session.execute(session.prepare(String.format("SELECT o_w_id, o_d_id, o_c_id from order_tab\n" +
                        "WHERE o_w_id = %d AND o_d_id= %d AND o_id = %d\n" +
                        "ALLOW FILTERING", relatedOrder.w_id, relatedOrder.d_id, relatedOrder.o_id)).bind().setConsistencyLevel(ConsistencyLevel.ONE)).one();

                relatedCustomerSet.add(
                        new Customer(relatedCustomer.getInt("o_w_id"),
                                relatedCustomer.getInt("o_w_id"),
                                relatedCustomer.getInt("o_w_id")));
            }

        }
        if (relatedCustomerSet.isEmpty()) {
            System.out.println("No related customer");
        } else {
            System.out.print("Related Customer: ");
            System.out.println(relatedCustomerSet);
        }


    }

    private static void getDbState(CqlSession session) throws Exception {
        System.out.println("====================== DB State ============================");
        StringBuilder res = new StringBuilder();

        Row warehouseRow = session.execute(session.prepare(
                        "SELECT sum(W_YTD) as w_ytd from schema_a.warehouse_tab")
                .bind()
                .setConsistencyLevel(USE_QUORUM)).one();
        BigDecimal w_ytd = warehouseRow.getBigDecimal("w_ytd");
        System.out.printf("W_YTD: %f\n", w_ytd);
        res.append(w_ytd.doubleValue() + "\n");

        Row districtResult = session.execute(session.prepare(
                        "SELECT sum(D_YTD) as d_ytd, sum(D_NEXT_O_ID) as d_next_o_ytd from district_tab;")
                .bind()
                .setConsistencyLevel(USE_QUORUM)).one();
        BigDecimal d_ytd = districtResult.getBigDecimal("d_ytd");
        int d_next_o_ytd = districtResult.getInt("d_next_o_ytd");

        System.out.printf("D_YTD: %f\n", d_ytd);
        res.append(d_ytd.doubleValue() + "\n");
        System.out.printf("D_NEXT_O_ID: %d\n", d_next_o_ytd);
        res.append(d_next_o_ytd + "\n");

        Row customerResult = session.execute(session.prepare(
                        "select sum(C_BALANCE) as c_balance, sum(C_YTD_PAYMENT) as c_tyd_payment, " +
                                "sum(C_PAYMENT_CNT) as c_payment_count , sum(C_DELIVERY_CNT) as c_delivery_cnt from Customer_tab;")
                .bind()
                .setConsistencyLevel(USE_QUORUM)).one();
        BigDecimal c_balance = customerResult.getBigDecimal("c_balance");
        System.out.printf("C_BALANCE: %f\n", c_balance);
        res.append(c_balance.doubleValue() + "\n");
        float c_tyd_payment = customerResult.getFloat("c_tyd_payment");
        System.out.printf("C_YTD_PAYMENT: %f\n", c_tyd_payment);
        res.append(c_tyd_payment + "\n");
        int c_payment_count = customerResult.getInt("c_payment_count");
        System.out.printf("C_PAYMENT_CNT: %d\n", c_payment_count);
        res.append(c_payment_count + "\n");
        int c_delivery_cnt = customerResult.getInt("c_delivery_cnt");
        System.out.printf("C_DELIVERY_CNT: %d\n", c_delivery_cnt);
        res.append(c_delivery_cnt + "\n");

        Row orderResult = session.execute(session.prepare(
                        "select max(O_ID) as o_id, sum(O_OL_CNT) as o_ol_cnt from Order_tab;")
                .bind()
                .setConsistencyLevel(USE_QUORUM)).one();
        int o_id = orderResult.getInt("o_id");
        BigDecimal o_ol_cnt = orderResult.getBigDecimal("o_ol_cnt");

        System.out.printf("O_ID: %d\n", o_id);
        res.append(o_id + "\n");
        System.out.printf("O_OL_CNT: %f\n", o_ol_cnt);
        res.append(o_ol_cnt.doubleValue() + "\n");

        Row orderLineRow = session.execute(
                session.prepare("select sum(OL_AMOUNT) as ol_amount, sum(OL_QUANTITY) as ol_quantity from Order_line_tab;")
                        .bind()
                        .setConsistencyLevel(USE_QUORUM)).one();
        BigDecimal ol_amount = orderLineRow.getBigDecimal("ol_amount");
        BigDecimal ol_quantity = orderLineRow.getBigDecimal("ol_quantity");

        System.out.printf("OL_AMOUNT: %f\n", ol_amount);
        res.append(ol_amount.doubleValue() + "\n");
        System.out.printf("OL_QUANTITY: %f\n", ol_quantity);
        res.append(ol_quantity.doubleValue() + "\n");

        DriverConfig config = session.getContext().getConfig();

        Row stockRow = session.execute(session.prepare(
                        "select sum(S_QUANTITY) as s_quantity, sum(S_YTD) as s_ytd, sum(S_ORDER_CNT) as s_order_cnt," +
                                " sum(S_REMOTE_CNT) as s_remote_cnt from Stock_tab;")
                .bind().setConsistencyLevel(USE_QUORUM).setExecutionProfile(config.getProfile("slow"))).one();
        BigDecimal s_quantity = stockRow.getBigDecimal("s_quantity");
        System.out.printf("S_QUANTITY: %f\n", s_quantity);
        res.append(s_quantity.doubleValue() + "\n");
        BigDecimal s_ytd = stockRow.getBigDecimal("s_ytd");
        System.out.printf("S_YTD: %f\n", s_ytd);
        res.append(s_ytd.doubleValue() + "\n");
        int s_order_cnt = stockRow.getInt("s_order_cnt");
        System.out.printf("S_ORDER_CNT: %d\n", s_order_cnt);
        res.append(s_order_cnt + "\n");
        int s_remote_cnt = stockRow.getInt("s_remote_cnt");
        System.out.printf("S_REMOTE_CNT: %d\n", s_remote_cnt);
        res.append(s_remote_cnt + "\n");


        System.out.println("======================DB State Plain=======================");
        System.out.println(res);
        TransactionStatistics.printServerTime();
        System.out.println();
    }
}
