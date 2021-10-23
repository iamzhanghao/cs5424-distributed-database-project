package clients;

import org.postgresql.ds.PGSimpleDataSource;

import java.io.FileInputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Scanner;

public class CockroachDB {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("run the program by: ./CockroachDB <host> <port> <database> <data_dir>");
        }
        System.out.println(args[0]);
        System.out.println(args[1]);
        System.out.println(args[2]);
        System.out.println(args[3]);
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String database = args[2];
        String dataDir = args[3];


        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName("localhost");
        ds.setPortNumber(26257);
        ds.setDatabaseName("schema_a");
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

        System.out.println("Ready to ready xact files");

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
                float payment = Float.parseFloat(splits[4]);
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
//        System.out.println("Hi");

    }

    private static void paymentTransaction(Connection conn, int cwid, int cdid, int cid, float payment) {

    }

    private static void deliveryTransaction(Connection conn, int wid, int carrierid) {

    }

    private static void orderStatusTransaction(Connection conn, int cwid, int cdid, int cid) {

    }

    private static void stockLevelTransaction(Connection conn, int wid, int did, int t, int l) {

    }

    private static void popularItemTransaction(Connection conn, int wid, int did, int l) {

    }

    private static void topBalanceTransaction(Connection conn) {

    }

    private static void relatedCustomerTransaction(Connection conn, int cwid, int cdid, int cid) {

    }
}
