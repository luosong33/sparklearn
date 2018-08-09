package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;
import yyyq.util.RowKeyUtil;

import java.io.*;
import java.sql.*;

public class Suolun2Hbase {

    //  接收数组入库
    public static void insertIntoHbase(String voucherNo, JSONObject job, String mobile_local) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"hulu_suolun\" (\"ID\",\"credit_card_repayment_cnt\", \"offline_cash_loan_cnt\", " +
                    "\"offline_installment_cnt\", \"online_cash_loan_cnt\", \"online_installment_cnt\",\"others_cnt\", " +
                    "\"payday_loan_cnt\", \"loadtime\") values (?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        try {
            setStmt(voucherNo, mobile_local, stmt, job);
            stmt.executeUpdate();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  组装插入语句
    private static void setStmt(String voucherNo, String mobile_local, PreparedStatement stmt, JSONObject job) throws SQLException {
        stmt.setString(1, RowKeyUtil.reverseStr(mobile_local) + "_" + voucherNo);
        stmt.setString(2, String.valueOf(job.get("credit_card_repayment_cnt")));
        stmt.setString(3, String.valueOf(job.get("offline_cash_loan_cnt")));
        stmt.setString(4, String.valueOf(job.get("offline_installment_cnt")));
        stmt.setString(5, String.valueOf(job.get("online_cash_loan_cnt")));
        stmt.setString(6, String.valueOf(job.get("online_installment_cnt")));
        stmt.setString(7, String.valueOf(job.get("others_cnt")));
        stmt.setString(8, String.valueOf(job.get("payday_loan_cnt")));
        stmt.setString(9, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
    }

    //  读文件解析
    public static void readFileByLines(String voucherNo, String fileName) {
//        System.out.println("========start========" + fileName);
        File file = null;
        try {
//            SshUtil.getContext("192.168.15.196","root","yinghuo#123", fileName);  //  本地测试
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
                        JSONObject data = (JSONObject) jobj.get("data");
                        String user_phone = (String) data.get("user_phone");
                        JSONObject history_org = (JSONObject) data.get("history_org");
                        insertIntoHbase(voucherNo, history_org, user_phone);

                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("filepath=============="+fileName);
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
        sql = "SELECT a.EX_SERIAL,b.FILE_PATH FROM cdsp_network_crawl_task a,cdsp_allwin_credit b WHERE file_path IS NOT NULL AND a.TASK_NO = b.TASK_NO " +
                " AND a.API_CODE = 'SAURON_ACCESS_SUMMIT' AND a.GMT_CREATED BETWEEN '" + args[0] + "' AND '" + args[1] + "' ";  //  索伦  "2017-09-18 00:00:00" "2017-09-19 00:00:00"
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String voucher = rs.getString("EX_SERIAL");  //  葫芦、摩羯
                String filePath = rs.getString("FILE_PATH");
                readFileByLines(voucher, filePath);
                System.out.println(DateUtil.nowString()+" ==Call=files=" + ++files+"==filepath=="+filePath);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString()+" SAURON_ACCESS_SUMMIT 耗时为： " + (endtime - starttime));
    }
}