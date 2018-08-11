package yyyq.datasync.jingshuang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import yyyq.util.DateUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/* 原始入库与user表撞到的装了盈盈易贷的用户 1660条 */
public class YyydInfoUser {

    public static void main(String[] args) throws Exception {
        long starttime = System.currentTimeMillis();

        String path = "D:\\workspace\\yyyd_info_user.csv";
        StringBuffer sb = new StringBuffer();
        try {
            FileInputStream inputStream = new FileInputStream(path);
            //  字节流中指定编码
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String s = null;
            ArrayList<Map<String, String>> list = new ArrayList<>();
            while ((s = bufferedReader.readLine()) != null) {
                //  逻辑
                HashMap<String, String> hashMap = new HashMap<>();

                String[] split = s.split(",");
                hashMap.put("client_no",split[0]);
                hashMap.put("mobile",split[1]);
                list.add(hashMap);
            }
            putBach("yyyd_info_user",list);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " YyydInfoUser 耗时为： " + (endtime - starttime));
    }

    // 批量插入
    public static void putBach(String tableName, List<Map<String, String>> listMap) {
        try {
            Configuration conf = HBaseConfiguration.create(); // 获得配制文件对象
            conf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection conn = ConnectionFactory.createConnection(conf);
            Table table = conn.getTable(TableName.valueOf(tableName));
            Admin admin = conn.getAdmin();
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
            conn.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}