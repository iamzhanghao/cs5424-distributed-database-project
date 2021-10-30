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
import java.util.ArrayList;
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
            dataDir = "project_files/xact_files_A/"+client+".txt";
        }else if(Objects.equals(schema, "B")){
            schema_name = "schema_a";
            dataDir = "project_files/xact_files_B/"+client+".txt";
        }else{
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
        while (scanner.hasNextLine() && txnCount<TXN_LIMIT) {
            txnCount++;
            String line = scanner.nextLine();
            String[] splits = line.split(",");
            char txnType = splits[0].toCharArray()[0];
            long latency = invokeTransaction(session, splits, scanner);
            latencies.add(new TransactionStatistics(txnType,latency));
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
                ArrayList<ArrayList<Integer>> orderItems = new ArrayList<>();
                int numItems = Integer.parseInt(splits[4]);
                for (int i = 0; i < numItems; i++) {
                    String line = scanner.nextLine();
                    splits = line.split(",");
                    ArrayList<Integer> item = new ArrayList<>();
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

    }

    private static void stockLevelTransaction(CqlSession session, int wid, int did, int t, int l) {

    }

    private static void popularItemTransaction(CqlSession session, int wid, int did, int l) {

    }

    private static void topBalanceTransaction(CqlSession session) {

    }

    private static void relatedCustomerTransaction(CqlSession session, int cwid, int cdid, int cid) {

    }
}
