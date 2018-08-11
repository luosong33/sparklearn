package yyyq.datasync.bk;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import yyyq.util.DateUtil;
import yyyq.util.SnowflakeIdGenerator;

import java.io.*;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class Zhima2Hbase {

    public static void reads(String inpath, String fileEncoding) {
        File file = new File(inpath);
        if (file.isDirectory()) {   //  文件夹
            File[] files = file.listFiles();
            if (files.length == 0) System.out.println("目录是空的!");
            else
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        reads(file2.getAbsolutePath(), fileEncoding);   // 子目录 返回递归
                    } else {
                        String file2path = file2.getAbsolutePath();
                        try {
                            handle(fileEncoding, file2path);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
        } else {    // 传入单个文件
            try {
                handle(fileEncoding, inpath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void handle(String fileEncoding, String file2path) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199");
        HTablePool pool = new HTablePool(conf, 10);
        HTableInterface table = pool.getTable("zhima_black_basic");

        FileInputStream inputStream = new FileInputStream(file2path);
        //  字节流中指定编码
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
        String s = null;
        StringBuffer sb = new StringBuffer();
        while ((s = bufferedReader.readLine()) != null) {
            //  逻辑
            JSONObject jsob = JSON.parseObject(s);
            String exSerial = String.valueOf(jsob.get("exSerial"));
            String bizNo = String.valueOf(jsob.get("bizNo"));
            String isMatched = String.valueOf(jsob.get("isMatched"));
            String success = String.valueOf(jsob.get("success"));

            JSONArray jarr = jsob.getJSONArray("details");
//            Connection connection = ConnectionFactory.createConnection(conf);
//            Table table = connection.getTable(TableName.valueOf("zhima_black_basic"));
            SnowflakeIdGenerator idWorker = new SnowflakeIdGenerator(0, 0);
            long id = idWorker.nextId();
            if (jarr != null) {
                int i = 0;
                for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
                    Put put = new Put(Bytes.toBytes(exSerial + "_" + id + i));
//                    ReadWriteUtil.write("/bigdata/ls/test.txt", exSerial + "_" + id + i);
                    i++;
                    JSONObject job = (JSONObject) iterator.next();
                    String bizCode = job.getString("bizCode");
                    String code = job.getString("code");
                    String level = job.getString("level");
                    String refreshTime = job.getString("refreshTime");
                    String settlement = job.getString("settlement");
                    String type = job.getString("type");
                    JSONArray extendInfoJarr = job.getJSONArray("extendInfo");
                    for (Iterator iterator_ = extendInfoJarr.iterator(); iterator_.hasNext(); ) {
                        JSONObject infoJob = (JSONObject) iterator_.next();
                        String key_ = infoJob.getString("key");
                        String value_ = infoJob.getString("value");
                        if (null != key_ && null != value_)
                            put.add("c".getBytes(), key_.getBytes(), Bytes.toBytes(value_));
                    }

                    if (null != bizCode) put.add("c".getBytes(), "bizCode".getBytes(), Bytes.toBytes(bizCode));
                    if (null != code) put.add("c".getBytes(), "code".getBytes(), Bytes.toBytes(code));
                    if (null != level) put.add("c".getBytes(), "level".getBytes(), Bytes.toBytes(level));
                    if (null != refreshTime) put.add("c".getBytes(), "refreshTime".getBytes(), Bytes.toBytes(refreshTime));
                    if (null != settlement) put.add("c".getBytes(), "settlement".getBytes(), Bytes.toBytes(settlement));
                    if (null != type) put.add("c".getBytes(), "type".getBytes(), Bytes.toBytes(type));

                    if (null != bizNo) put.add("c".getBytes(), "bizNo".getBytes(), Bytes.toBytes(bizNo));
                    if (null != isMatched) put.add("c".getBytes(), "isMatched".getBytes(), Bytes.toBytes(isMatched));
                    if (null != success) put.add("c".getBytes(), "success".getBytes(), Bytes.toBytes(success));
                    table.put(put);
                    table.flushCommits();
                    pool.putTable(table);
                }
            } else {
                Put put = new Put(Bytes.toBytes(exSerial + "_" + id));
//                ReadWriteUtil.write("/bigdata/ls/test.txt", exSerial + "_" + id);
                if (null != bizNo) put.add("c".getBytes(), "bizNo".getBytes(), Bytes.toBytes(bizNo));
                if (null != isMatched) put.add("c".getBytes(), "isMatched".getBytes(), Bytes.toBytes(isMatched));
                if (null != success) put.add("c".getBytes(), "success".getBytes(), Bytes.toBytes(success));
                table.put(put);
                table.flushCommits();
                pool.putTable(table);
            }
//            table.close();
        }
        bufferedReader.close();
        inputStream.close();
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
//        String filePath = "/tmp/nfs/cdsp/zhimaBlack/20171027";
//        String filePath = "D:\\tmp\\500120171027551836.txt";  //  59条
        String filePath = "D:\\tmp\\zhimaBlack";
        int files = 0;
        if (args.length == 1) {
            filePath = args[0];
            reads(filePath, "GBK");
            System.out.println(DateUtil.nowString()+" ==Call=files=" + ++files+"==filepath=="+filePath);
        } else if (args.length == 2) {
            List<String> dates = DateUtil.getBetweenDates(args[0], args[1], "yyyyMMdd", ""); // 20171017
            for (String date : dates) {
                filePath = "/tmp/nfs/cdsp/zhimaBlack/" + date;
//                filePath = "D:\\tmp\\zhimaBlack\\" + date;
                reads(filePath, "GBK");
                System.out.println(DateUtil.nowString()+" ==Call=files=" + ++files+"==filepath=="+filePath);
            }
        } else {
            String date = DateUtil.getFrontAfterDate(DateUtil.nowString("yyyyMMdd"), -1, "yyyyMMdd");
            filePath = "/tmp/nfs/cdsp/zhimaBlack/" + date;
            reads(filePath, "GBK");
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " Zhima2Hbase 耗时为： " + (endtime - starttime));
    }
}