package clients.utils;

import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class CockroachDbSQLConnectionHelper {
    private Connection conn;
    private PGSimpleDataSource ds;

    public Connection getConn() {
        return this.conn;
    }

    public CockroachDbSQLConnectionHelper(String host, int port, String schema_name) {
        this.ds = new PGSimpleDataSource();
        ds.setServerName(host);
        ds.setPortNumber(port);
        ds.setDatabaseName(schema_name);
        ds.setUser("root");
        ds.setPassword(null);
        ds.setSslMode("disable");
        ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
        ds.setApplicationName("CockroachDB App");
        this.connect();
    }

    public void connect() {
        try {
            this.conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                this.conn = this.ds.getConnection();
                this.conn.setAutoCommit(false);
                System.out.println("Connection to DB established!");
                break;
            } catch (SQLException e) {
                System.out.println("Connection to DB failed. Retry in 10 seconds...");
            }
            try {
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public void close() {
        try {
            this.conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
