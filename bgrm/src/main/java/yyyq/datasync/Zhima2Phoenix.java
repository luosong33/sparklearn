package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.*;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class Zhima2Phoenix {

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
                            System.out.println(DateUtil.nowString() + "=file2path=" + file2path);
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
    }

    private static void handle(String fileEncoding, String file2path) throws IOException {
//        SshUtil.getContext("192.168.15.196", "root", "yinghuo#123", "/tmp/nfs/cdsp/zhimaBlack/20171027/500120171027549693.txt");  //  本地测试
//        FileInputStream inputStream = new FileInputStream("D:\\tmp\\zhimaBlack\\20171028\\500120171111045556.txt");
        FileInputStream inputStream = new FileInputStream(file2path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
        String s = null;
        StringBuffer sb = new StringBuffer();
        while ((s = bufferedReader.readLine()) != null) {
            JSONObject jsob = JSON.parseObject(s);
            String exSerial = String.valueOf(jsob.get("exSerial"));
            String bizNo = String.valueOf(jsob.get("bizNo"));
            String isMatched = String.valueOf(jsob.get("isMatched"));
            String success = String.valueOf(jsob.get("success"));

            JSONArray jarr = jsob.getJSONArray("details");
            SnowflakeIdGenerator idWorker = new SnowflakeIdGenerator(0, 0);

            PreparedStatement stmt = null;
            Connection con = GetConnection.getPhoenixConn();
            if (jarr != null) {
                int i = 0;
                for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
                    long id = idWorker.nextId();
                    JSONObject job = (JSONObject) iterator.next();
                    String bizCode = job.getString("bizCode");
                    String code = job.getString("code");
                    String level = job.getString("level");
                    String refreshTime = job.getString("refreshTime");
                    String settlement = job.getString("settlement");
                    String type = job.getString("type");

                    try {
                        con.setAutoCommit(false);
                        stmt = con.prepareStatement("upsert  into \"zhima_black_basic\" (\"ID\",\"isMatched\", \"bizNo\", \"success\", " +
                                "\"level\", \"settlement\",\"refreshTime\", \"bizCode\", \"code\", \"type\", \"event_max_amt_code\", \"id\", " +
                                "\"event_end_time_desc\", \"loadtime\") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                        stmt.setString(1, null != exSerial ? exSerial + "_" + id + i : "");
                        i++;
                        stmt.setString(2, null != isMatched ? String.valueOf(isMatched) : "");
                        stmt.setString(3, null != bizNo ? String.valueOf(bizNo) : "");
                        stmt.setString(4, null != success ? String.valueOf(success) : "");
                        stmt.setString(5, null != level ? String.valueOf(level) : "");
                        stmt.setString(6, null != settlement ? String.valueOf(settlement) : "");
                        stmt.setString(7, null != refreshTime ? String.valueOf(refreshTime) : "");
                        stmt.setString(8, null != bizCode ? String.valueOf(bizCode) : "");
                        stmt.setString(9, null != code ? String.valueOf(code) : "");
                        stmt.setString(10, null != type ? String.valueOf(type) : "");
                        JSONArray extendInfoJarr = job.getJSONArray("extendInfo");
                        JSONObject jsob1 = extendInfoJarr != null && extendInfoJarr.size() > 0 ? (JSONObject) extendInfoJarr.get(0) : null;
                        JSONObject jsob2 = extendInfoJarr != null && extendInfoJarr.size() > 1 ? (JSONObject) extendInfoJarr.get(1) : null;
                        JSONObject jsob3 = extendInfoJarr != null && extendInfoJarr.size() > 2 ? (JSONObject) extendInfoJarr.get(2) : null;
                        /*int j = 11;
                        for (Iterator iterator_ = extendInfoJarr.iterator(); iterator_.hasNext(); ) {
                            JSONObject infoJob = (JSONObject) iterator_.next();
                            String value_ = infoJob.getString("value");
                            stmt.setString(j, null != value_ ? String.valueOf(value_) : "");
                            j++;
                        }*/
                        stmt.setString(11, null != jsob1 ? jsob1.getString("value") : "");
                        stmt.setString(12, null != jsob2 ? jsob2.getString("value") : "");
                        stmt.setString(13, null != jsob3 ? jsob3.getString("value") : "");
                        stmt.setString(14, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                        stmt.addBatch();
                        stmt.executeBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                try {
                    long id = idWorker.nextId();
                    con.setAutoCommit(false);
                    stmt = con.prepareStatement("upsert  into \"zhima_black_basic\" (\"ID\",\"isMatched\", \"bizNo\", \"success\", \"loadtime\") " +
                            "values (?,?,?,?,?)");
                    stmt.setString(1, exSerial + "_" + id);
                    if (null != isMatched) stmt.setString(2, String.valueOf(isMatched));
                    if (null != bizNo) stmt.setString(3, String.valueOf(bizNo));
                    if (null != success) stmt.setString(4, String.valueOf(success));
                    stmt.setString(5, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                    stmt.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
            try {
                stmt.executeBatch();
                con.commit();
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        bufferedReader.close();
        inputStream.close();
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        String filePath = "";
        int files = 0;

        List<String> dates = DateUtil.getBetweenDates(args[0], args[1], "yyyy-MM-dd HH:mm:ss", "yyyyMMdd"); // 20171017
        for (String date : dates) {
            filePath = "/tmp/nfs/cdsp/zhimaBlack/" + date;
//                filePath = "D:\\tmp\\zhimaBlack\\" + date;
            reads(filePath, "GBK");
            System.out.println(DateUtil.nowString() + " ==Call=files=" + ++files + "==filepath===" + filePath);
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " Zhima2Hbase 耗时为： " + (endtime - starttime));
    }
}