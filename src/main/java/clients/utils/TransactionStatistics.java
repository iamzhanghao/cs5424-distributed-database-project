package clients.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class TransactionStatistics {

    public char txnType;
    public Long latency;


    public TransactionStatistics(char type, Long latency) {
        this.txnType = type;
        this.latency = latency;
    }

    private static ArrayList<TransactionStatistics> getMeanLatencies(ArrayList<TransactionStatistics> latencies) {
        ArrayList<TransactionStatistics> result = new ArrayList<>();
        HashMap<Character, Integer> count = new HashMap<>();
        HashMap<Character, Long> latencySum = new HashMap<>();
        for (TransactionStatistics stat : latencies) {
            if (!count.containsKey(stat.txnType) && !latencySum.containsKey(stat.txnType)) {
                count.put(stat.txnType, 1);
                latencySum.put(stat.txnType, stat.latency);
            } else {
                count.replace(stat.txnType, count.get(stat.txnType) + 1);
                latencySum.replace(stat.txnType, latencySum.get(stat.txnType) + stat.latency);
            }
        }

        for (char key : count.keySet()) {
            result.add(new TransactionStatistics(key, latencySum.get(key) / count.get(key)));
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
        System.out.printf("Total %d transactions, avg latency %fms \n", count, (float)totalLatency / (float)count);
        for (TransactionStatistics stat : meanStats) {
            System.out.printf("Txn %c: avg latency %dms \n", stat.txnType, stat.latency);
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
        builder.append(columnNamesList +"\n");
        builder.append("1"+",");
        builder.append("Chola");
        builder.append('\n');
        pw.write(builder.toString());
        pw.close();
        System.out.println("done!");
    }

}
