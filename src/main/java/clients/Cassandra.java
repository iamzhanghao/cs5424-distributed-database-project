package clients;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringJoiner;

public class Cassandra {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("run the program by: ./Cassandra <host> <port> <schema_name> <data_dir>");
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String schema = args[2];
        String dataDir = args[3];

        System.out.printf("Running on host: %s:%d", host, port);
        System.out.println();

        CqlSession session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withKeyspace(schema)
//                .withLocalDatacenter("cs5424-c")
                .withLocalDatacenter("datacenter1")
                .build();

        FileInputStream stream = new FileInputStream(dataDir);
        Scanner scanner = new Scanner(stream);

        ArrayList<Long> latencies = new ArrayList<Long>();
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] splits = line.split(",");
            latencies.add(invokeTransaction(session, splits, scanner));
        }

        session.close();
    }

    private static long invokeTransaction(CqlSession session, String[] splits, Scanner scanner) {
        long start = System.currentTimeMillis();
        switch (splits[0].toCharArray()[0]) {
            case 'N':
                int cid = Integer.parseInt(splits[1]);
                int wid = Integer.parseInt(splits[2]);
                int did = Integer.parseInt(splits[3]);
                ArrayList<ArrayList<Integer>> orderItems = new ArrayList<ArrayList<Integer>>();
                int numItems = Integer.parseInt(splits[4]);
                for (int i = 0; i < numItems; i++) {
                    String line = scanner.nextLine();
                    splits = line.split(",");
                    ArrayList<Integer> item = new ArrayList<Integer>();
                    for (String element : splits) {
                        item.add(Integer.parseInt(element));
                    }
                    orderItems.add(item);
                }
                newOrderTransaction(session, cid, wid, did, orderItems);
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

    private static void newOrderTransaction(CqlSession session, int cid, int wid, int did, ArrayList<ArrayList<Integer>> orderItems) {

    }

    private static void paymentTransaction(CqlSession session, int cwid, int cdid, int cid, BigDecimal payment) {
        try{
            ResultSet warehouse_result  = session.execute(
                "SELECT w_ytd FROM warehouse_tab WHERE w_id = " + cwid + " ;");
            Row warehouseRow = warehouse_result.one();
            BigDecimal old_ytd = warehouseRow.getBigDecimal("w_ytd");
            PreparedStatement updateWarehouse = session.prepare(
                "UPDATE warehouse_tab SET W_YTD = ? WHERE W_ID = ?;");
            BoundStatement updateWarehouseBound = updateWarehouse.bind()
                    .setBigDecimal(0,   old_ytd.add(payment))
                    .setInt(1, cwid);
            session.execute(updateWarehouseBound);

            ResultSet customer_result = session.execute(
                "SELECT c_balance, c_ytd_payment, c_payment_cnt FROM customer_tab " +
                        String.format("WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d ;",cwid,cdid,cid));
            Row customer_row = customer_result.one();
            BigDecimal old_c_balance = customer_row.getBigDecimal("c_balance");
            float old_c_ytd_payment = customer_row.getFloat("c_ytd_payment");
            int old_c_payment_cnt = customer_row.getInt("c_payment_cnt");

            PreparedStatement updateCustomer = session.prepare(
                "Update customer_tab " +
                        "SET c_balance = ?,c_ytd_payment = ?, c_payment_cnt = ? " +
                        "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? ;");
            BoundStatement updateCustomerBound = updateCustomer.bind()
                    .setBigDecimal(0,old_c_balance.subtract(payment))
                    .setFloat(1,old_c_ytd_payment+payment.floatValue())
                    .setInt(2,old_c_payment_cnt+1)
                    .setInt(3,cwid)
                    .setInt(4,cdid)
                    .setInt(5,cid);
            session.execute(updateCustomerBound);

        }catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void deliveryTransaction(CqlSession session, int wid, int carrierid) {

    }

    private static void orderStatusTransaction(CqlSession session, int cwid, int cdid, int cid) {
        try {
            String get_customer = "select c_first, c_middle, c_last, c_balance from customer_tab "
                + "where c_w_id = %d and c_d_id = %d and c_id = %d ";
            String get_last_order = "SELECT o_w_id, o_d_id, o_c_id, o_id, o_entry_d, o_carrier_id "
                + "FROM order_tab "
                + "WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d order by o_id desc LIMIT 1 ";
            String get_order_items = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
                + "from order_line_tab where ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d ";

            // customer last order
            Row customer  = session.execute(String.format(get_customer, cwid, cdid, cid)).one();
            Row last_order = session.execute(String.format(get_last_order, cwid, cdid, cid)).one();
            Integer last_order_id = last_order.getInt("o_id");

            System.out.printf("Customer name: %s %s %s, Balance: %f\n",
                customer.getString("c_first"),
                customer.getString("c_middle"),
                customer.getString("c_last"),
                customer.getBigDecimal("c_balance").doubleValue());
            System.out.printf("Customer last order id: %d, Entry Datetime: %s, Carrier id: %d\n",
                last_order_id,
                last_order.getString("o_entry_d"),
                last_order.getInt("o_carrier_id"));

            // order items
            ResultSet rs_order_items = session.execute(String.format(get_order_items, cwid, cdid, last_order_id));
            for(Row item : rs_order_items){
                System.out.printf("Item id: %d, Warehouse id: %d, Quantity: %d, Price: %d, Delivery Datetime: %s\n", 
                item.getInt("ol_i_id"), 
                item.getInt("ol_supply_w_id"), 
                item.getInt("ol_quantity"), 
                item.getInt("ol_amount"), 
                item.getString("ol_delivery_d"));
            }

        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void stockLevelTransaction(CqlSession session, int wid, int did, int t, int l) {

    }

    private static void popularItemTransaction(CqlSession session, int wid, int did, int l) {
        String get_N = "select d_next_o_id from district_tab where d_w_id = %d and d_id = %d";
        String get_order = "select o_id, o_entry_d, o_c_id from order_tab where o_w_id = %d and o_d_id = %d and o_id in ( %s )";
        String get_order_lines = "select ol_o_id, ol_i_id, ol_quantity from order_line_tab "
            + "where ol_w_id = %d and ol_d_id = %d and ol_o_id in ( %s )";
        String get_customer = "select c_id, c_first, c_middle, c_last from customer_tab "
            + "where c_w_id = %d and c_d_id = %d and c_id in ( %s )";
        String get_items = "select i_id, i_name from item_tab where i_id in ( %s )";
        

        try {
            Integer N = session.execute(String.format(get_N, wid, did)).one().getInt("d_next_o_id");

            StringJoiner o_ids = new StringJoiner(",");
            for(int i = N-l; i <N; i++){
                o_ids.add((N-i)+"");
            }
            
            List<Row> orders = session.execute(String.format(get_order, wid, did, o_ids.toString())).all();
            List<Row> order_lines = session.execute(String.format(get_order_lines, wid, did, o_ids.toString())).all();
            
            StringJoiner c_ids = new StringJoiner(",");
            for(Row order : orders){
                c_ids.add(order.getInt("o_c_id")+"");
            }

            List<Row> customers = session.execute(String.format(get_customer, wid, did, c_ids.toString())).all();

            System.out.printf("WID: %d, DID: %d, Number of last orders: %d\n", wid, did, l);
            System.out.println();

            
            Set<Integer> all_popular_items = new HashSet<>();
            List<Set<Integer>> popular_items_each_order = new ArrayList<Set<Integer>>();
            Map<Integer, Row> customer_map = new HashMap<>();
            Map<Integer, List<Row>> ol_map = new HashMap<>();
            Map<Integer, String> item_map = new HashMap<>();

            // group order_line by o_id
            for (Row ol : order_lines){
                Integer ol_o_id = ol.getInt("ol_o_id");
                if (!ol_map.containsKey(ol_o_id)){
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
            for(Row customer : customers){
                customer_map.put(customer.getInt("c_id"), customer);
            }

            for(Row order : orders){
                Integer o_id = order.getInt("o_id");
                Row customer = customer_map.get(order.getInt("o_c_id"));
                System.out.printf(String.format("OID: %d, O_ENTRY_D: %s, Customer Name: %s\n", 
                    o_id, 
                    order.getString("o_entry_d")),
                    customer.getString("c_first")+" "+customer.getString("c_middle")+" "+customer.getString("c_last"));

                List<Row> ols = ol_map.get(o_id);
                Map<Integer, Integer> quantity_map = new HashMap<>();
                for(Row ol : ols){
                    Integer i_id = ol.getInt("ol_i_id");
                    Integer ol_quantity = ol.getInt("ol_quantity");
                    if (!quantity_map.containsKey(i_id)){
                        quantity_map.put(i_id, ol_quantity);
                    } else {
                        quantity_map.put(i_id, quantity_map.get(i_id) + ol_quantity);
                    }
                }
                Integer max_quantity = Integer.MIN_VALUE;
                for(Map.Entry<Integer, Integer> q : quantity_map.entrySet()){
                    max_quantity = Math.max(max_quantity, q.getValue());
                }

                Set<Integer> i_ids = new HashSet<>();
                for(Row ol : ols){
                    if (ol.getInt("ol_quantity") == max_quantity) {
                        all_popular_items.add(ol.getInt("ol_i_id"));
                        i_ids.add(ol.getInt("ol_i_id"));
                    }
                }

                StringJoiner i_ids_str = new StringJoiner(",");
                for(Integer i_id : i_ids){
                    i_ids_str.add(i_id+"");
                }

                List<Row> items = session.execute(String.format(get_items, i_ids_str.toString())).all();
                for(Row item : items){
                    item_map.put(item.getInt("i_id"), item.getString("i_name"));
                    System.out.printf("Popular I_NAME: %s, quantity: %d\n", 
                        item.getString("i_name"), 
                        max_quantity);
                }

                popular_items_each_order.add(i_ids);
            
            }

            for(Integer i_id : all_popular_items){
                int count = 0;
                for(Set<Integer> items : popular_items_each_order){
                    if (items.contains(i_id)){
                        count ++;
                    }
                }
                System.out.printf("Popular I_NAME: %s, Percentage of Orders having Popular Items: %f\n", 
                    item_map.get(i_id), 
                    (float) count * 1 / orders.size());
            }

        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    private static void topBalanceTransaction(CqlSession session) {

    }

    private static void relatedCustomerTransaction(CqlSession session, int cwid, int cdid, int cid) {

    }
}
