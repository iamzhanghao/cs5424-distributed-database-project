package clients.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class TransactionStatistics {

    public char txnType;
    public float latency;
    public float retry;


    public TransactionStatistics(char type, float latency, float retry) {
        this.txnType = type;
        this.latency = latency;
        this.retry = retry;
    }

    private static ArrayList<TransactionStatistics> getMeanLatencies(ArrayList<TransactionStatistics> latencies) {
        ArrayList<TransactionStatistics> result = new ArrayList<>();
        HashMap<Character, Integer> count = new HashMap<>();
        HashMap<Character, Float> latencySum = new HashMap<>();
        HashMap<Character, Float> retrySum = new HashMap<>();
        for (TransactionStatistics stat : latencies) {
            if (!count.containsKey(stat.txnType) && !latencySum.containsKey(stat.txnType)) {
                count.put(stat.txnType, 1);
                latencySum.put(stat.txnType, stat.latency);
                retrySum.put(stat.txnType, stat.retry);
            } else {
                count.replace(stat.txnType, count.get(stat.txnType) + 1);
                latencySum.replace(stat.txnType, latencySum.get(stat.txnType) + stat.latency);
                retrySum.replace(stat.txnType, retrySum.get(stat.txnType) + stat.retry);

            }
        }

        for (char key : count.keySet()) {
            result.add(new TransactionStatistics(key, latencySum.get(key) / count.get(key),retrySum.get(key)/count.get(key)));
        }

        return result;
    }

    public static void printStatistics(ArrayList<TransactionStatistics> stats) {
        ArrayList<TransactionStatistics> meanStats = getMeanLatencies(stats);
        System.out.println("============Statistics============");
        int count = 0;
        long totalLatency = 0;
        for (TransactionStatistics stat : stats) {
            count += 1;
            totalLatency += stat.latency;
        }
        System.out.printf("Total %d transactions, avg latency %.2fms \n", count, (float) totalLatency / (float) count);
        for (TransactionStatistics stat : meanStats) {
            System.out.printf("Txn %c: avg latency %.2fms, avg retries %.2f times  \n", stat.txnType, stat.latency,stat.retry);
        }
        System.out.println();
    }

    public static void writeStatisticsToCsv(ArrayList<TransactionStatistics> stats) {
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new File("NewData.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder builder = new StringBuilder();
        String columnNamesList = "Id,Name";
        // No need give the headers Like: id, Name on builder.append
        builder.append(columnNamesList + "\n");
        builder.append("1" + ",");
        builder.append("Chola");
        builder.append('\n');
        pw.write(builder.toString());
        pw.close();
        System.out.println("done!");
    }

}
