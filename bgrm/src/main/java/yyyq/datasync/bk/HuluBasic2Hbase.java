package yyyq.datasync.bk;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;
import yyyq.util.RowKeyUtil;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class HuluBasic2Hbase {

    //  接收数组入库
    public static void insertIntoHbase(String tablename, String voucherNo, JSONArray jarr, String mobile_local) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            //  葫芦  bops_client_call_record_gourd
            stmt = con.prepareStatement("upsert  into \"" + tablename + "\" (\"ID\",\"contact_noon\", \"phone_num_loc\", \"contact_3m\", \"contact_1m\", \"contact_1w\", " +
                    "\"p_relation\", \"mobile_answer\", \"contact_name\", \"call_in_cnt\", \"call_out_cnt\", \"call_out_len\", \"contact_holiday\", \"needs_type\", \"contact_weekday\", " +
                    "\"contact_afternoon\", \"call_len\", \"contact_early_morning\", \"contact_night\", \"contact_3m_plus\", \"call_cnt\", \"call_in_len\", \"contact_all_day\", " +
                    "\"contact_morning\", \"contact_weekend\", \"mobile_local\", \"voucher_no\", \"loadtime\") " +
                    "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        int count = 0;
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            try {
                setStmt(voucherNo, mobile_local, stmt, job);
                stmt.addBatch();
                count++;
            } catch (SQLException e) {
                e.printStackTrace();
            }

            if (count % 1000 == 0) {
                try {
                    stmt.executeBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
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

    //  组装插入语句
    private static void setStmt(String voucherNo, String mobile_local, PreparedStatement stmt, JSONObject job) throws SQLException {
        stmt.setString(1, RowKeyUtil.reverseStr(mobile_local) + "_" + voucherNo + "_" + String.valueOf(job.get("phone_num")));
        stmt.setString(2, String.valueOf(job.get("contact_noon")));
        stmt.setString(3, String.valueOf(job.get("phone_num_loc")));
        stmt.setString(4, String.valueOf(job.get("contact_3m")));
        stmt.setString(5, String.valueOf(job.get("contact_1m")));
        stmt.setString(6, String.valueOf(job.get("contact_1w")));
        stmt.setString(7, String.valueOf(job.get("p_relation")));
        stmt.setString(8, String.valueOf(job.get("phone_num")));
        stmt.setString(9, String.valueOf(job.get("contact_name")));
        stmt.setString(10, String.valueOf(job.get("call_in_cnt")));
        stmt.setString(11, String.valueOf(job.get("call_out_cnt")));
        stmt.setString(12, String.valueOf(job.get("call_out_len")));
        stmt.setString(13, String.valueOf(job.get("contact_holiday")));
        stmt.setString(14, String.valueOf(job.get("needs_type")));
        stmt.setString(15, String.valueOf(job.get("contact_weekday")));
        stmt.setString(16, String.valueOf(job.get("contact_afternoon")));
        stmt.setString(17, String.valueOf(job.get("call_len")));
        stmt.setString(18, String.valueOf(job.get("contact_early_morni")));
        stmt.setString(19, String.valueOf(job.get("contact_night")));
        stmt.setString(20, String.valueOf(job.get("contact_3m_plus")));
        stmt.setString(21, String.valueOf(job.get("call_cnt")));
        stmt.setString(22, String.valueOf(job.get("call_in_len")));
        stmt.setString(23, String.valueOf(job.get("contact_all_day")));
        stmt.setString(24, String.valueOf(job.get("contact_morning")));
        stmt.setString(25, String.valueOf(job.get("contact_weekend")));
        stmt.setString(26, mobile_local);
        stmt.setString(27, voucherNo);
        stmt.setString(28, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));

    }

    //  读文件解析
    public static void readFileByLines(String voucherNo, String fileName) {
//        System.out.println("========start========" + fileName);
        File file = null;
        try {
//            SshUtil.getContext("192.168.15.196","root","yinghuo#123", fileName);
//            file = new File("d:/tmp/b.txt");
            file = new File(fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (file != null) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
                String tempString = null;
                while ((tempString = reader.readLine()) != null) {
                    try {
                        JSONObject jobj = JSON.parseObject(tempString);
                        JSONArray jarr = null;
                        String mobile = "";
                        //  葫芦
                        JSONObject pushDataJson = (JSONObject) jobj.get("push_data");
                        jarr = pushDataJson.getJSONArray("contact_list");  //  数组
                        mobile = (String) jobj.get("cell_phone_number");  //  取本人手机号
                        insertIntoHbase("hulu_report", voucherNo, jarr, mobile); // bops_client_call_record_gourd bops_client_call_record_gourd_test

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        int files = 0;
        Connection conn = GetConnection.getConn_Cdsp_Riskcore();
        String sql = "";
            sql = "select VOUCHER_NO,FILE_PATH from cdsp_hulu_access where QUERY_TYPE='REPORT' and  file_path IS NOT NULL "
                    + "and GMT_CREATED >= '" + args[0] + "' " + "and GMT_CREATED < '" + args[1] + "' ";  //  葫芦通讯详单//  增量  and GMT_CREATED >= '2017-09-06 00:00:00'
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String voucher =  rs.getString("VOUCHER_NO");  //  葫芦、摩羯
                String filePath = rs.getString("FILE_PATH");
                readFileByLines(voucher, filePath);
                System.out.println("=================Call=files==================" + ++files);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString()+" hulu_report 耗时为： " + (endtime - starttime));
    }
}