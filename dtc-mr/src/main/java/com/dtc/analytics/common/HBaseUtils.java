package com.dtc.analytics.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 2019-05-13
 *
 * @author :hao.li
 */
public class HBaseUtils {

    private final static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);
    private static Configuration configuration;
    private static Connection connection;

    private static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", configuration.get("hbase.zookeeper.port"));
        configuration.set("hbase.zookeeper.quorum", configuration.get("hbase.zookeeper.quorum"));
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("hbase.zookeeper.quorum", "10.3.6.7,10.3.6.12,10.3.6.16");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            logger.error("Failed to create hbase connection , the cause is {}.", e);
        }
    }

    //create table
    public void createTable(Connection conn) throws IOException {
        Admin admin = conn.getAdmin();
        System.out.println("[hbaseoperation] start createtable...");

        String tableNameString = "table_book";
        TableName tableName = TableName.valueOf(tableNameString);
        if (admin.tableExists(tableName)) {
            System.out.println("[INFO] table exist");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor("columnfamily_1"));
            hTableDescriptor.addFamily(new HColumnDescriptor("columnfamily_2"));
            hTableDescriptor.addFamily(new HColumnDescriptor("columnfamily_3"));
            admin.createTable(hTableDescriptor);
        }

        System.out.println("[hbaseoperation] end createtable...");
    }

    public static void insterRow(String tableName, String rowkey, String colFamily, String col, String val) {
        logger.info("starting to insert rusult into hbase ...");
        init();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
            table.put(put);
        } catch (IOException e) {
            logger.error("failed insert to hbase and the cause is {}.", e);
        } finally {

            // 批量插入
            /*
             * List<Put> putList = new ArrayList<Put>(); puts.add(put);
             * table.put(putList);
             */
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            close();
        }
        logger.info("end to insert rusult into hbase.");
    }

    // insert data
    public static void insert() throws IOException {
        System.out.println("[hbaseoperation] start insert...");

        Table table = connection.getTable(TableName.valueOf("ns_dataflow:dataflow_es_error"));
        List<Put> putList = new ArrayList<Put>();

        Put put1;
        put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("<<Java In Action>>"));
        Put put2;
        put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("event"), Bytes.toBytes("name"), Bytes.toBytes("<<C++ Prime>>"));
        Put put3;
        put3 = new Put(Bytes.toBytes("row3"));
        put3.addColumn(Bytes.toBytes("event"), Bytes.toBytes("name"), Bytes.toBytes("<<Hadoop in Action>>"));

        putList.add(put1);
        putList.add(put2);
        putList.add(put3);
        table.put(put1);

        System.out.println("[hbaseoperation] start insert...");
    }

    //queryTable
    public void queryTable() throws IOException {
        System.out.println("[hbaseoperation] start queryTable...");

        Table table = connection.getTable(TableName.valueOf("table_book"));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            byte[] row = result.getRow();
            System.out.println("row key is:" + Bytes.toString(row));

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {
                System.out.print("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                System.out.print("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.print("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                System.out.println("Timestamp:" + cell.getTimestamp());
            }
        }

        System.out.println("[hbaseoperation] end queryTable...");
    }

    public void queryTableByRowKey(String rowkey) throws IOException {
        System.out.println("[hbaseoperation] start queryTableByRowKey...");

        Table table = connection.getTable(TableName.valueOf("table_book"));
        Get get = new Get(rowkey.getBytes());
        Result result = table.get(get);

        List<Cell> listCells = result.listCells();
        for (Cell cell : listCells) {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            long timestamp = cell.getTimestamp();
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " + timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
        }

        System.out.println("[hbaseoperation] end queryTableByRowKey...");
    }


    public void queryTableByCondition(String authorName) throws IOException {
        System.out.println("[hbaseoperation] start queryTableByCondition...");

        Table table = connection.getTable(TableName.valueOf("table_book"));
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(authorName));
        Scan scan = new Scan();

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                long timestamp = cell.getTimestamp();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " + timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
            }
        }

        System.out.println("[hbaseoperation] end queryTableByCondition...");
    }

    // 删除数据
    public void deleRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        // 删除指定列族
        // delete.addFamily(Bytes.toBytes(colFamily));
        // 删除指定列
        // delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        // 批量删除
        /*
         * List<Delete> deleteList = new ArrayList<Delete>();
         * deleteList.add(delete); table.delete(deleteList);
         */
        table.close();
        close();
    }

    public void deleteColumnFamily(String cf) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("table_book");
        admin.deleteColumn(tableName, Bytes.toBytes(cf));
    }

    public void deleteByRowKey(String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf("table_book"));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        queryTable();
    }

    public void truncateTable() throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("table_book");

        admin.disableTable(tableName);
        admin.truncateTable(tableName, true);
    }

    public void deleteTable() throws IOException {
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf("table_book"));
        admin.deleteTable(TableName.valueOf("table_book"));
    }

    //关闭连接
    public static void close() {

        try {
            Admin admin = connection.getAdmin();
            if (null != admin)
                admin.close();
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 查看已有表
    public void listTables() throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    public static Configuration getConfiguration() {
        return configuration;
    }

    public static void setConfiguration(Configuration configuration) {
        HBaseUtils.configuration = configuration;
    }

    public static Connection getConnection() {
        return connection;
    }

    public static void setConnection(Connection connection) {
        HBaseUtils.connection = connection;
    }


}
