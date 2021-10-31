package clients;

import org.postgresql.ds.PGSimpleDataSource;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class CockroachDB {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("run the program by: ./CockroachDB <host> <port> <schema_name> <data_dir>");
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String schema = args[2];
        String dataDir = args[3];

        System.out.printf("Running on host: %s:%d", host, port);
        System.out.println();

        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName(host);
        ds.setPortNumber(port);
        ds.setDatabaseName(schema);
        ds.setUser("root");
        ds.setPassword(null);
        ds.setSsl(true);
        ds.setSslMode("require");
        ds.setSslCert("certs/client.root.crt");
        ds.setSslKey("certs/client.root.key.pk8");
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("CockroachDB App");

        Connection conn = ds.getConnection();
        conn.setAutoCommit(false);
        FileInputStream stream = new FileInputStream(dataDir);
        Scanner scanner = new Scanner(stream);

        System.out.println("Ready to read Xact file "+ dataDir);

        ArrayList<Long> latencies = new ArrayList<Long>();
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] splits = line.split(",");
            latencies.add(invokeTransaction(conn, splits, scanner));
        }
    }

    private static long invokeTransaction(Connection conn, String[] splits, Scanner scanner) {
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
                newOrderTransaction(conn, cid, wid, did, orderItems);
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

    private static void newOrderTransaction(Connection conn, int cid, int wid, int did, ArrayList<ArrayList<Integer>> orderItems) {


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
            updateDistrict.setBigDecimal(1,payment);
            updateDistrict.setInt(2,cwid);
            updateDistrict.setInt(3,cdid);
            updateDistrict.executeUpdate();

            PreparedStatement updateCustomer = conn.prepareStatement(
                    "UPDATE customer_tab " +
                    "SET C_BALANCE= C_BALANCE - ?, C_YTD_PAYMENT= C_YTD_PAYMENT + ?, C_PAYMENT_CNT= C_PAYMENT_CNT + 1 " +
                    "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
            updateCustomer.setBigDecimal(1,payment);
            updateCustomer.setBigDecimal(2,payment);
            updateCustomer.setInt(3,cwid);
            updateCustomer.setInt(4,cdid);
            updateCustomer.setInt(5,cid);
            updateCustomer.executeUpdate();

            conn.commit();

        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]", e.getSQLState(), e.getCause(),
                    e.getMessage());
        }
    }

    private static void deliveryTransaction(Connection conn, int wid, int carrierid) {

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
            
            if (rs_customer_last_order.next()){
                int last_order_id = rs_customer_last_order.getInt("o_id");

                System.out.printf("Customer name: %s %s %s, Balance: %f\n",
                rs_customer_last_order.getString("c_first"),
                rs_customer_last_order.getString("c_middle"),
                rs_customer_last_order.getString("c_last"),
                rs_customer_last_order.getBigDecimal("c_balance").doubleValue());  
    
                System.out.printf("Customer last order id: %d, Entry Datetime: %s, Carrier id: %d\n",
                last_order_id,
                rs_customer_last_order.getString("o_entry_d"),
                rs_customer_last_order.getInt("o_carrier_id"));
                
                //Order items
                ResultSet rs_order_items = st.executeQuery(String.format(get_order_items, cwid, cdid, last_order_id));
                
                while(rs_order_items.next()){
                    System.out.printf("Item id: %d, Warehouse id: %d, Quantity: %d, Price: %d, Delivery Datetime: %s\n", 
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

    private static void stockLevelTransaction(Connection conn, int wid, int did, int t, int l) {

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
                    +"from order_line_tab ol_sum "
                    +"where ol_sum.ol_w_id = %d and ol_sum.ol_d_id = %d "
                    +"group by ol_sum.ol_w_id, ol_sum.ol_d_id, ol_sum.ol_o_id, ol_sum.ol_i_id";

        String get_ol_quantity_max = "select ol_max.ol_o_id, max(ol_max.sum_quantity) "
                    +"from ( "
                        + String.format(get_ol_quantity_sum, wid, did)
                    +" ) ol_max "
                    +"group by ol_max.ol_w_id, ol_max.ol_d_id, ol_max.ol_o_id ";

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

        try{
            Statement st = conn.createStatement();
            ResultSet rs_popular_items = st.executeQuery(String.format(get_popular_items, wid, did, l));

            Map<Integer, ArrayList<String>> orders = new HashMap<Integer, ArrayList<String>>();
            Map<Integer, Set<Integer>> items = new HashMap<Integer, Set<Integer>>();
            Map<Integer, String> order_descs = new HashMap<Integer, String>();
            Map<Integer, String> item_descs = new HashMap<Integer, String>();

            while(rs_popular_items.next()){
                int oid = rs_popular_items.getInt("ol_o_id");
                int iid = rs_popular_items.getInt("i_id");
                String iname = rs_popular_items.getString("i_name");
                String popular_item_desc = String.format("Popular I_NAME: %s, quantity: %d\n", iname, rs_popular_items.getInt("quantity"));
                String order_customer_desc = String.format("OID: %d, O_ENTRY_D: %s, Customer Name: %s\n", oid, rs_popular_items.getString("o_entry_d"), rs_popular_items.getString("c_name"));
                // get popular items for each order
                if (!orders.containsKey(oid)){
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
                    item_descs.put(iid,iname);
                } else {
                    Set<Integer> oids = items.get(iid);
                    oids.add(oid);
                    items.put(iid, oids);
                }

            }

            // Output results
            System.out.printf("WID: %d, DID: %d, Number of last orders: %d\n", wid, did, l);
            System.out.println();

            for(int oid : orders.keySet()){
                System.out.printf(order_descs.get(oid));
                for(String desc : orders.get(oid)){
                    System.out.printf(desc);
                }
                System.out.println();
            }

            for(int iid : items.keySet()){
                System.out.printf("Popular I_NAME: %s, Percentage of Orders having Popular Items: %f\n", item_descs.get(iid), (float) items.get(iid).size() * 1 / orders.size());
            }

            rs_popular_items.close();

        }catch(SQLException e){
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]", e.getSQLState(), e.getCause(),
            e.getMessage());
        }
    }

    private static void topBalanceTransaction(Connection conn) {

    }

    private static void relatedCustomerTransaction(Connection conn, int cwid, int cdid, int cid) {
        System.out.println("Related Customer");
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT DISTINCT w_id_1 as c_w_id, d_id_1 as c_d_id, c_id_1 as c_id " +
                    "FROM (SELECT o_w_id as w_id_1, o_c_id as c_id_1,  o_d_id as d_id_1, ol_i_id as i_1" +
                    "      FROM order_tab  ,order_line_tab " +
                    "      WHERE o_w_id = ol_w_id AND o_d_id = ol_d_id AND o_id = ol_o_id AND o_w_id <> "+cwid+") AS a, " +
                    "     (SELECT o_w_id as w_id_2, o_c_id as c_id_2,  o_d_id as d_id_2, ol_i_id as i_2" +
                    "      FROM order_tab  ,order_line_tab " +
                    "      WHERE o_w_id = ol_w_id AND o_d_id = ol_d_id AND o_id = ol_o_id AND o_w_id <> "+cwid+") AS b " +
                    "WHERE w_id_1 = w_id_2 AND c_id_1 = c_id_2 AND d_id_1 = d_id_2 AND i_1 <> i_2 " +
                    "" +
                    "AND i_1 IN (SELECT DISTINCT ol_i_id as c_items " +
                    "            FROM order_tab , order_line_tab " +
                    "            WHERE o_w_id = ol_w_id " +
                    "            AND o_d_id = ol_d_id " +
                    "            AND o_id = ol_o_id " +
                    "            AND o_w_id = " + cwid +
                    "            AND o_d_id = " + cdid +
                    "            AND o_c_id = " + cid +")" +
                    "" +
                    "AND i_2 IN (SELECT DISTINCT ol_i_id as c_items " +
                    "            FROM order_tab , order_line_tab " +
                    "            WHERE o_w_id = ol_w_id " +
                    "            AND o_d_id = ol_d_id " +
                    "            AND o_id = ol_o_id " +
                    "            AND o_w_id = " + cwid +
                    "            AND o_d_id = " + cdid +
                    "            AND o_c_id = " + cid +")"
            );

            while (rs.next()) {
                int r_cwid = rs.getInt(1);
                int r_cdid = rs.getInt(2);
                int r_cid = rs.getInt(3);

                System.out.printf("CWID: %d, C_DID: %d, C_ID: %d" , r_cwid, r_cdid, r_cid);
                System.out.println();
            }
            rs.close();

        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }

    }

}
