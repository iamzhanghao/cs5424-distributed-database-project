package clients.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.rmi.server.ExportException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TransactionStatistics {

    public char txnType;
    public float latency;
    public float retry;

    public TransactionStatistics(char type, float latency, float retry) {
        this.txnType = type;
        this.latency = latency;
        this.retry = retry;
    }

    private static void printMeanLatencyForEachQuery(ArrayList<TransactionStatistics> latencies) {
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
            result.add(new TransactionStatistics(key, latencySum.get(key) / count.get(key), retrySum.get(key) / count.get(key)));
        }

        for (TransactionStatistics stat : result) {
            System.out.printf("Txn %c: avg latency %.2fms, retry %.2f times  \n", stat.txnType, stat.latency, stat.retry);
        }
        System.out.println();
    }

    public static double percentile(double[] latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.length);
        return latencies[index - 1];
    }

    public static String getStatistics(ArrayList<TransactionStatistics> stats, float clientTotalTime, String clientId, String csvFilePath) {
        System.out.println("========================Statistics========================");
        double statValues[] = new double[stats.size()];
        for (int i = 0; i < stats.size(); i++) {
            statValues[i] = stats.get(i).latency;
        }
        Arrays.sort(statValues);


        int numberOfTransactions = stats.size();
        float totalExcutionTimeSeconds = clientTotalTime;
        float throughput = numberOfTransactions / totalExcutionTimeSeconds;
        DoubleSummaryStatistics doubleStats = Arrays.stream(statValues).summaryStatistics();
        double median;
        if (statValues.length % 2 == 0)
            median = (statValues[statValues.length / 2] + statValues[statValues.length / 2 - 1]) / 2;
        else
            median = statValues[statValues.length / 2];

        System.out.printf("Total %d transactions, execution time %.2fs, avg latency %.2fms, throughput %.2f query/s,\n" +
                        "median latency %.2fms, 95th percentile latency %.2fms, 99th percentile latency %.2fms  \n",
                numberOfTransactions, totalExcutionTimeSeconds, doubleStats.getAverage(), throughput,
                median, percentile(statValues, 95), percentile(statValues, 99));
        System.out.println();
        printMeanLatencyForEachQuery(stats);
        System.out.println("==========================================================");
        writeStatisticsToCsv(clientId, numberOfTransactions, totalExcutionTimeSeconds, doubleStats.getAverage(), throughput,
                median, percentile(statValues, 95), percentile(statValues, 99), csvFilePath);
        return String.format("Statistics: %d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f",
                numberOfTransactions, totalExcutionTimeSeconds, doubleStats.getAverage(), throughput, median,
                percentile(statValues, 95), percentile(statValues, 99));
    }

    public static void writeCsvHeader(String csvFilePath){
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new FileOutputStream(csvFilePath));
            String line = String.format("Client ID,Transaction Count,Total Execution Time,Mean Latency,Throughput,Median,95th Percentile,99th Percentile\n");
            pw.write(line);
            pw.close();
            System.out.printf("Successfully wrote header to %s!", csvFilePath);
            System.out.println();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void writeStatisticsToCsv(String clientId, int numberOfTransactions, float totalExcutionTimeSeconds,
                                            double mean, double throughput, double median, double percentile95,
                                            double percentile99, String csvFilePath) {
        PrintWriter pw = null;
        while (true) {
            try {
                pw = new PrintWriter(new FileOutputStream(csvFilePath, true));
                String line = String.format("%s,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n",
                        clientId, numberOfTransactions, totalExcutionTimeSeconds, mean, throughput, median, percentile95, percentile99);
                pw.write(line);
                pw.close();
                System.out.printf("Successfully wrote results to %s!", csvFilePath);
                System.out.println();
                break;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }

    }

    public static void printServerTime() {
        System.out.println("Current Time at server: " + new SimpleDateFormat("YYYY-mm-dd HH:mm:ss").format(Calendar.getInstance().getTime()));
    }
}
