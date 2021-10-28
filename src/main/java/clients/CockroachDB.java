package clients;

import jnr.ffi.annotations.In;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.text.DecimalFormat;
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
                int numItems = Integer.parseInt(splits[4]);

                ArrayList<Integer> items = new ArrayList<Integer>();
                ArrayList<Integer> supplierWarehouses = new ArrayList<Integer>();
                ArrayList<Integer> quantities = new ArrayList<Integer>();
                for (int i = 0; i < numItems; i++) {
                    String line = scanner.nextLine();
                    splits = line.split(",");
                    items.add(Integer.parseInt(splits[0]));
                    supplierWarehouses.add(Integer.parseInt(splits[1]));
                    quantities.add(Integer.parseInt(splits[2]));
                }
                newOrderTransaction(conn, cid, wid, did, numItems, items, supplierWarehouses, quantities);
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

    private static void newOrderTransaction(Connection conn, int cid, int wid, int did, int number_of_items,
                                            ArrayList<Integer> items, ArrayList<Integer> supplier_warehouses, ArrayList<Integer> quantities) {
        try {

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM district_tab WHERE D_W_ID = "+wid+" AND D_ID = "+did+"limit 1");
            Integer next_order_id = 0;
            while (rs.next()) {
                next_order_id = rs.getInt("D_NEXT_O_ID");
                break;
            }

            next_order_id+=1;

            PreparedStatement updateDistrict = conn.prepareStatement(
                "UPDATE district_tab SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?");
            updateDistrict.setInt(1, next_order_id);
            updateDistrict.setInt(2,wid);
            updateDistrict.setInt(3,did);
            updateDistrict.executeUpdate();

            int all_local = 0;
            if (supplier_warehouses.stream().distinct().count() <= 1 && supplier_warehouses.contains(wid)) {
                all_local = 1;
            }

            PreparedStatement createOrder = conn.prepareStatement("insert into order_tab (O_W_ID, O_D_ID, O_ID, O_C_ID," +
                    " O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (?,?,?,?,?,?,?,?;");
            createOrder.setInt(1, wid);
            createOrder.setInt(2, did);
            createOrder.setInt(3, next_order_id);
            createOrder.setInt(4, cid);
            createOrder.setInt(5, 0);
            createOrder.setInt(6, number_of_items);
            createOrder.setInt(7, all_local);
            Date date = new Date(System.currentTimeMillis());
            createOrder.setDate(8, date);
            createOrder.execute();

            double total_amount = 0;
            ArrayList<String> itemNames = new ArrayList<String>();
            ArrayList<Float> itemPrices = new ArrayList<Float>();
            ArrayList<Integer> itemStocks = new ArrayList<Integer>();

            for (int idx = 0; idx < items.size(); idx++) {
                int current_item = items.get(idx);

                rs = stmt.executeQuery("SELECT w_tax FROM item_tab WHERE i_id = "+current_item);
                float price = 0;
                String name = "";
                while (rs.next()) {
                    price = rs.getFloat("i_price");
                    name = rs.getString("i_name");
                    break;
                }

                itemNames.add(name);
                itemPrices.add(price);

                PreparedStatement itemStock = conn.prepareStatement("select s_quantity,s_ytd,s_order_cnt," +
                        "s_remote_cnt,s_dist_? from item_stock where w_id=? and i_id=?;");
                String strDid = "";
                if (did == 10) {
                    strDid =  String.valueOf(did);
                } else {
                    strDid = "0"+ did;
                }
                itemStock.setString(1, strDid);
                itemStock.setInt(2, wid);
                itemStock.setInt(3, current_item);
                rs = itemStock.executeQuery();

                int s_quantity = 0;
                float s_ytd = 0;
                int s_order_cnt = 0;
                int s_remote_cnt = 0;

                while (rs.next()) {
                    s_quantity = rs.getInt("s_quantity");
                    s_ytd = rs.getFloat("s_ytd");
                    s_order_cnt = rs.getInt("s_order_cnt");
                    s_remote_cnt = rs.getInt("s_remote_cnt");
                    break;
                }
                int adjusted_quantity = s_quantity - quantities.get(idx);
                if (adjusted_quantity<10) {
                    adjusted_quantity+=100;
                }
                if (supplier_warehouses.get(idx) != wid) {
                    s_remote_cnt += 1;
                }

                PreparedStatement updateStock = conn.prepareStatement("update item_stock set " +
                        "s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? where w_id=? and i_id=?;");
                updateStock.setInt(1, adjusted_quantity);
                updateStock.setFloat(2, s_ytd+quantities.get(idx));
                updateStock.setInt(3, s_order_cnt+1);
                updateStock.setInt(4, s_remote_cnt);
                updateStock.setInt(5, wid);
                updateStock.setInt(6, current_item);
                updateStock.executeUpdate();

                itemStocks.add(adjusted_quantity);

                float item_amount = price * quantities.get(idx);
                total_amount += item_amount;

                PreparedStatement insertOrderLine = conn.prepareStatement("insert into order_line_tab (OL_W_ID, OL_D_ID, " +
                        "OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) " +
                        "VALUES (?,?,?,?,?,'{}',?,?,?,?);");
                insertOrderLine.setInt(1, wid);
                insertOrderLine.setInt(2, did);
                insertOrderLine.setInt(3, next_order_id);
                insertOrderLine.setInt(4, idx);
                insertOrderLine.setInt(5, current_item);
                insertOrderLine.setFloat(6, item_amount);
                insertOrderLine.setInt(7, supplier_warehouses.get(idx));
                insertOrderLine.setFloat(8, quantities.get(idx));
                insertOrderLine.setString(9, "S_DIST_"+ did);
                insertOrderLine.execute();
            }

            double discount = 0;
            String last_name = "";
            String credit = "";
            rs = stmt.executeQuery("SELECT c_last, c_credit, c_discount FROM customer_tab WHERE c_id = "+cid+" AND c_d_id = "+did);
            while (rs.next()) {
                discount = rs.getInt("c_discount");
                last_name = rs.getString("c_last");
                credit = rs.getString("c_credit");
                break;
            }

            double warehouse_tax_rate = 0;
            rs = stmt.executeQuery("SELECT w_tax FROM warehouse_tab WHERE w_id = "+wid);
            while (rs.next()) {
                warehouse_tax_rate = rs.getInt("w_tax");
                break;
            }

            double district_tax_rate = 0;
            rs = stmt.executeQuery("SELECT d_tax FROM district_tab WHERE d_id = "+did+" AND d_w_id = "+wid);
            while (rs.next()) {
                district_tax_rate = rs.getInt("d_tax");
                break;
            }
            rs.close();

            total_amount = total_amount*(1+warehouse_tax_rate+district_tax_rate)*(1-discount);
            conn.commit();

            System.out.printf("WarehouseID %d, DistrictID: %d, CustomerID: %d \n" , wid, did, cid);
            System.out.printf("LastName %s, Credit: %s, Discount: %s \n" , last_name, credit, discount);
            System.out.printf("WarehouseTaxRate %s, DistrictTaxRate: %s \n" , warehouse_tax_rate, district_tax_rate);
            System.out.printf("OrderID %s, OrderEntryDate: %s, NumberOfItems \n" , next_order_id, "", number_of_items);
            System.out.printf("TotalAmount %s \n", total_amount);
            for (int idx = 0; idx < items.size(); idx++) {
                System.out.printf("ItemID %s, SupplierWarehouse %s, Quantity %s", items.get(idx), supplier_warehouses.get(idx), quantities.get(idx));
                System.out.printf("ItemName %s, ItemAmount %s, StockQuantity %s", itemNames.get(idx), itemPrices.get(idx)*quantities.get(idx), itemStocks.get(idx));
            }
            System.out.println();

        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]", e.getSQLState(), e.getCause(),
                    e.getMessage());
        }
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

            PreparedStatement updateCustomer_1 = conn.prepareStatement(
                    "UPDATE customer_tab " +
                    "SET C_BALANCE= C_BALANCE - ?" +
                    "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
            updateCustomer_1.setBigDecimal(1,payment);
            updateCustomer_1.setInt(2,cwid);
            updateCustomer_1.setInt(3,cdid);
            updateCustomer_1.setInt(4,cid);
            updateCustomer_1.executeUpdate();

            PreparedStatement updateCustomer_2 = conn.prepareStatement(
                    "UPDATE customer_tab " +
                    "SET C_YTD_PAYMENT= C_YTD_PAYMENT + ?" +
                    "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
            updateCustomer_2.setBigDecimal(1,payment);
            updateCustomer_2.setInt(2,cwid);
            updateCustomer_2.setInt(3,cdid);
            updateCustomer_2.setInt(4,cid);
            updateCustomer_2.executeUpdate();

            PreparedStatement updateCustomer_3 = conn.prepareStatement(
                    "UPDATE customer_tab " +
                    "SET C_PAYMENT_CNT= C_PAYMENT_CNT + 1" +
                    "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
            updateCustomer_3.setInt(1,cwid);
            updateCustomer_3.setInt(2,cdid);
            updateCustomer_3.setInt(3,cid);
            updateCustomer_3.executeUpdate();

            conn.commit();

        } catch (SQLException e) {
            System.out.printf("sql state = [%s]cause = [%s]message = [%s]", e.getSQLState(), e.getCause(),
                    e.getMessage());
        }
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
