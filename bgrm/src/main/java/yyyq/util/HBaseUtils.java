package yyyq.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {
    public static Configuration configuration;
    public static Admin admin;
    public static Connection connection;

    private HBaseUtils() {
    }

    static {
        System.setProperty("HADOOP_USER_NAME", "root");
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.15.195");

        try {
            if (connection == null) {
                connection = ConnectionFactory.createConnection(configuration);
            }
            if (admin == null) {
                admin = connection.getAdmin();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table getHbaseTbale(String tableName) {
        Table table = null;
        try {
            table(tableName);  //  判断表是否存在，如果不存在进行创建
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public static void close() {
        try {
            /*if (null != table) {
                table.close();
            }*/
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //  判断表是否存在，如果不存在进行创建
    public static void table(String tableName) {
        try {
            if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor hc = new HColumnDescriptor(Bytes.toBytes("c"));
                htd.addFamily(hc);
                admin.createTable(htd);
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void insert(Table table, List<Map<String, String>> recordMapList) throws IOException {
        List<Put> putList = new ArrayList<>();
        for (Map<String, String> recordMap : recordMapList) {
            String rowKey = recordMap.get("rowKey");
            Put put = new Put(Bytes.toBytes(rowKey));
            recordMap.remove("rowKey");
            for (String key : recordMap.keySet()) {
                String value = String.valueOf(recordMap.get(key));
                put.addColumn(Bytes.toBytes("c"), Bytes.toBytes(key), Bytes.toBytes(value));
            }
            putList.add(put);
        }
        table.put(putList);
    }

    public static void truncate(String table) throws IOException {
        admin.disableTable(TableName.valueOf(table));
        admin.truncateTable(TableName.valueOf(table), true);
        admin.close();
    }

    public static void deleteCreate(String table) throws IOException {
        HTableDescriptor td = admin.getTableDescriptor(TableName.valueOf(Bytes.toBytes(table)));

        admin.disableTable(TableName.valueOf(table));
        admin.deleteTable(TableName.valueOf(table));

        admin.createTable(td);
    }


    // *批量插入
    public static void putBach(String tableName, List<Map<String, String>> listMap) {
        try {
            Configuration conf = HBaseConfiguration.create(); // 获得配制文件对象
            conf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Admin admin = connection.getAdmin();
            //判断表是否存在，如果不存在进行创建
            if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor hc = new HColumnDescriptor(Bytes.toBytes("c"));
                htd.addFamily(hc);
                admin.createTable(htd);
            }
            admin.close();
            //进行数据插入
            List<Put> putList = new ArrayList<>();
            int i = 0;
            for (Map<String, String> map : listMap) {
                Put put = new Put(Bytes.toBytes(String.valueOf(i)));
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    put.add(Bytes.toBytes(String.valueOf("c")), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
                putList.add(put);
                i++;
            }
            table.put(putList);
            table.close();
            connection.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // *批量插入
    public static void putBach_(String tableName, List<Map<String, String>> listMap) {
        try {
            Configuration conf = HBaseConfiguration.create(); // 获得配制文件对象
            conf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Admin admin = connection.getAdmin();
            //判断表是否存在，如果不存在进行创建
            if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor hc = new HColumnDescriptor(Bytes.toBytes("c"));
                htd.addFamily(hc);
                admin.createTable(htd);
            }
            admin.close();
            //进行数据插入
            List<Put> putList = new ArrayList<>();
            for (Map<String, String> map : listMap) {
                String rowKey = map.get("rowKey");
                Put put = new Put(Bytes.toBytes(String.valueOf(rowKey)));
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    put.add(Bytes.toBytes(String.valueOf("c")), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
                putList.add(put);
            }
            table.put(putList);
            table.close();
            connection.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
//            HBaseUtils hBaseUtils = new HBaseUtils();
//            HBaseUtils.truncate("mojie_report_creditcard_repayment");
            HBaseUtils.deleteCreate("mojie_report_creditcard_repayment");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
