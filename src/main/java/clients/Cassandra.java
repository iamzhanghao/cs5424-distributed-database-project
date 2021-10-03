package clients;

import com.datastax.oss.driver.api.core.CqlSession;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Scanner;

public class Cassandra {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("run the program by: ./Cassandra <host> <port> <keyspace> <data_dir>");
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String database = args[2];
        String dataDir = args[3];

        CqlSession session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter("cs5424-c")
                .withKeyspace(database)
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
                float payment = Float.parseFloat(splits[4]);
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

    private static void paymentTransaction(CqlSession session, int cwid, int cdid, int cid, float payment) {

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
