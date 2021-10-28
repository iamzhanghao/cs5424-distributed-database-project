package clients;

import org.postgresql.ds.PGSimpleDataSource;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;

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

        System.out.println("Ready to read Xact file " + dataDir);

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
            updateDistrict.setBigDecimal(1, payment);
            updateDistrict.setInt(2, cwid);
            updateDistrict.setInt(3, cdid);
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

    }

    private static void stockLevelTransaction(Connection conn, int wid, int did, int t, int l) {

    }

    private static void popularItemTransaction(Connection conn, int wid, int did, int l) {

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

            while(rs.next()) {
                String firstName = rs.getString("c_first");
                String middleName = rs.getString("c_middle");
                String lastName = rs.getString("c_last");
                BigDecimal balance = rs.getBigDecimal("c_balance");
                String warehouseName = rs.getString("w_name");
                String districtName = rs.getString("d_name");

                System.out.printf("Customer Name: %-36s\tBalance: %-12.2f\tWarehouse Name: %-10s\tDistrict Name: %-10s\n",
                        firstName+' '+middleName+' '+ lastName, balance, warehouseName, districtName);
            }
        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]",
                    e.getSQLState(), e.getCause(), e.getMessage());
        }

    }

    private static void relatedCustomerTransaction(Connection conn, int cwid, int cdid, int cid) {
        System.out.println("Related Customer");
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
