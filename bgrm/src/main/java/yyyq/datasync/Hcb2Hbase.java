package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Table;
import yyyq.util.*;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class Hcb2Hbase {


    public static void reads(String inpath, String fileEncoding) {
        File file = new File(inpath);
        if (file.exists()) {  //  无论是对文件或文件夹的操作和判断都要先校验存在性，否则不存在目录会进入到文件操作（39行）
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
                    System.out.println(DateUtil.nowString() + "=inpath=" + inpath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("file not exists, create it ...");
            try {
                file.createNewFile();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    static int count = 0;

    private static void handle(String fileEncoding, String file2path) throws IOException {
        System.out.println(DateUtil.nowString() + " ==Call=files=" + file2path + "================"+ count++);
        FileInputStream inputStream = new FileInputStream(file2path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
        String s = null;
        StringBuffer sb = new StringBuffer();
        while ((s = bufferedReader.readLine()) != null) {
            List<Map<String, String>> lists = new ArrayList<>();
            HashMap<String, String> hashmap = new HashMap<>();

            JSONObject job = JSON.parseObject(s);
            String respData = job.getString("respData");
            JSONObject jobg = JSON.parseObject(respData);
            Set<String> keySet = jobg.keySet();
            for (String key : keySet) {
                hashmap.put(key, jobg.getString(key));
            }
            String rowKey = jobg.getString("ic_no");
            SnowflakeIdGenerator snowflakeIdGenerator = new SnowflakeIdGenerator(1, 1);
            if ("".equals(rowKey)) {
                long l = snowflakeIdGenerator.nextId();
                rowKey = l + "";
            }
//            rowKey += "_" + count;
            hashmap.put("rowKey", rowKey);
            hashmap.put("loadtime", DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
            lists.add(hashmap);

            Table table = HBaseUtils.getHbaseTbale("hcb");
            HBaseUtils.insert(table, lists);
            table.close();
        }
        bufferedReader.close();
        inputStream.close();
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        String filePath = "";
        List<String> dates = DateUtil.getBetweenDates(args[0], args[1], "yyyy-MM-dd HH:mm:ss", "yyyyMMdd"); // 20171017
        for (String date : dates) {
            filePath = "/tmp/nfs/cdsp/hcb/" + date;
//            System.out.println(DateUtil.nowString() + " ==Call=files=================" + filePath + "================");
//            filePath = "D:\\workspace\\hcb\\" + date;
            reads(filePath, "GBK");

        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " Hcb2Hbase 耗时为： " + (endtime - starttime));
    }
}