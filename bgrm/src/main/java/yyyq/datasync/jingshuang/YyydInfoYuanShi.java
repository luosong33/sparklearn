package yyyq.datasync.jingshuang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
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

/* 入库原始失联数据  5878条  */
public class YyydInfoYuanShi {

    public static void main(String[] args) throws Exception {
        long starttime = System.currentTimeMillis();

        String path = "D:\\tmp\\yyyd_info.csv";
        StringBuffer sb = new StringBuffer();
        try {
            FileInputStream inputStream = new FileInputStream(path);
            //  字节流中指定编码
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String s = null;
            ArrayList<Map<String, String>> list = new ArrayList<>();
            while ((s = bufferedReader.readLine()) != null) {
                //  逻辑
                String[] split = s.split(",");
                HashMap<String, String> hashMap = new HashMap<>();

                hashMap.put("client_no",split[0]);
                hashMap.put("tender_name",split[1]);
                hashMap.put("req_no",split[2]);
                hashMap.put("party_type",split[3]);
                hashMap.put("client_name",split[4]);
                list.add(hashMap);
            }
            putBach("yyyd_info",list);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " YyydInfoYuanShi 耗时为： " + (endtime - starttime));
    }

    // 批量插入
    public static void putBach(String tableName, List<Map<String, String>> listMap) {
        try {
            Configuration conf = HBaseConfiguration.create(); // 获得配制文件对象
            conf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection conn = ConnectionFactory.createConnection(conf);
            Table table = conn.getTable(TableName.valueOf(tableName));
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