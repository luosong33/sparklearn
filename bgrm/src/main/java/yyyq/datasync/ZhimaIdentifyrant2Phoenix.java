package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class ZhimaIdentifyrant2Phoenix {

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
//        FileInputStream inputStream = new FileInputStream("d:/tmp/zhima_identifyFrant.txt");
        FileInputStream inputStream = new FileInputStream(file2path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
        String s = null;
        StringBuffer sb = new StringBuffer();
        Connection con = null;
        PreparedStatement stmt = null;
        while ((s = bufferedReader.readLine()) != null) {
            JSONObject jsob = JSON.parseObject(s);
            String exSerial = String.valueOf(jsob.get("exSerial"));
            String ivsScore = String.valueOf(jsob.get("ivsScore"));
            String bizNo = String.valueOf(jsob.get("bizNo"));
            String success = String.valueOf(jsob.get("success"));
            JSONArray ivsDetail = (JSONArray) jsob.get("ivsDetail");
            con = GetConnection.getPhoenixConn();
            try {
                con.setAutoCommit(false);
                stmt = con.prepareStatement("upsert  into \"zhima_identi_fyFrant\" (\"ID\",\"ivsScore\", \"bizNo\", \"success\", \"ivsDetail_1_code\", " +
                        " \"ivsDetail_1_description\", \"ivsDetail_2_code\", \"ivsDetail_2_description\", \"ivsDetail_3_code\", \"ivsDetail_3_description\", " +
                        "\"ivsDetail_4_code\", \"ivsDetail_4_description\", \"loadtime\") " +
                        "values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
                stmt.setString(1, null != exSerial ? String.valueOf(exSerial) : "");
                stmt.setString(2, null != ivsScore ? String.valueOf(ivsScore) : "");
                stmt.setString(3, null != bizNo ? String.valueOf(bizNo) : "");
                stmt.setString(4, null != success ? String.valueOf(success) : "");
                int i = 5;
                for (Iterator iterator = ivsDetail.iterator(); iterator.hasNext();) {
                    JSONObject infoJob = (JSONObject) iterator.next();
                    String code = infoJob.getString("code");
                    String description = infoJob.getString("description");
                    stmt.setString(i, null != code ? code : "");
                    stmt.setString(++i, null != description ? description : "");
                    i++;
                }
                stmt.setString(13, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                stmt.addBatch();
                stmt.executeBatch();
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
        bufferedReader.close();
        inputStream.close();
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        String filePath = "";
        int files = 0;
        List<String> dates = DateUtil.getBetweenDates(args[0], args[1], "yyyy-MM-dd HH:mm:ss", "yyyyMMdd"); // 20171017
        for (String date : dates) {
            filePath = "/tmp/nfs/cdsp/zhimaIdentifyFrant/" + date;
            reads(filePath, "GBK");
            System.out.println(DateUtil.nowString() + " ==Call=files=" + ++files + "==filepath==" + filePath);
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " ZhimaIdentifyrant2Phoenix 耗时为： " + (endtime - starttime));
    }
}