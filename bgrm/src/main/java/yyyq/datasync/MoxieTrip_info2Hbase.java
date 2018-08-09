package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Table;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;
import yyyq.util.HBaseUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class MoxieTrip_info2Hbase {


    public static void readFileByLines(String filePath, String voucher_no, String client_no, String cert_no) {
        File file = null;
        try {
            file = new File(filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (file != null) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
                String tempString = null;
                while ((tempString = reader.readLine()) != null) {
                    handleInsert(filePath, voucher_no, client_no, cert_no, tempString);
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

    private static void handleInsert(String filePath, String voucher_no, String client_no, String cert_no, String tempString) {
        try {
            JSONObject jobj = JSON.parseObject(tempString);

            JSONArray trip_info = jobj.getJSONArray("trip_info");
            for (Iterator iterator = trip_info.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                insertJSONObject2Hbase("mojie_report_tripinfo", job, cert_no, voucher_no, client_no);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(filePath);
        }
    }


    private static void insertJSONObject2Hbase(String tableName, JSONObject job, String cert_no, String voucher_no, String client_no) throws Exception {
        List<Map<String, String>> lists = new ArrayList<>();
        HashMap<String, String> hashmap = new HashMap<>();
        Set<String> keySet = job.keySet();
        for (String key : keySet) {
            hashmap.put(key, job.getString(key));
        }
        hashmap.put("rowKey", client_no + "_" + cert_no + "_" + job.getString("trip_start_time"));
        hashmap.put("cert_no", cert_no);
        hashmap.put("voucher_no", voucher_no);
        hashmap.put("client_no", client_no);
        hashmap.put("loadtime", DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
        lists.add(hashmap);

        Table table = HBaseUtils.getHbaseTbale(tableName);
        HBaseUtils.insert(table, lists);
        table.close();
    }


    static int files = 0;

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        Connection conn = GetConnection.getConn_Cdsp_Riskcore();
        Connection conn_ = GetConnection.getConn_Bops_Sales();
        String sql = "SELECT VOUCHER_NO,FILE_PATH FROM cdsp_hulu_access n WHERE QUERY_TYPE = 'MOXIE_CARRIER_REPORT' and  file_path IS NOT NULL  " +
                "AND n.GMT_CREATED BETWEEN '" + args[0] + "' AND '" + args[1] + "' ";
        PreparedStatement pps;
        try {
            pps = conn.prepareStatement(sql);
            ResultSet rs = pps.executeQuery();
            while (rs.next()) {
                String voucher_no = rs.getString("VOUCHER_NO");     // 授信编号
                String filePath = rs.getString("FILE_PATH");        // 文件路径
                String sql_ = "SELECT client_no FROM bops.bops_loan_request r WHERE REQ_no = '" + voucher_no + "'; ";
                pps = conn_.prepareStatement(sql_);
                ResultSet rs_ = pps.executeQuery();
                getFiles(conn_, voucher_no, filePath, rs_);  //  将结果传入方法循环并校验

                if (flag == 0) {
                    sql_ = "SELECT client_no FROM bops.bops_credit_request r WHERE credit_no = '" + voucher_no + "'; ";
                    pps = conn_.prepareStatement(sql_);
                    rs_ = pps.executeQuery();
                    getFiles(conn_, voucher_no, filePath, rs_);
                }
                flag = 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
            conn_.close();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " MoxieTrip_info2Hbase导入耗时为： " + (endtime - starttime));
    }

    static int flag = 0;

    private static void getFiles(Connection conn_, String voucher_no, String filePath, ResultSet rs_) throws SQLException {
        PreparedStatement pps;
        while (rs_.next()) {
            flag = 1;  //  代表客户号有值
            String client_no = rs_.getString("client_no");   // 客户编号
            String _sql = "SELECT CERT_NO FROM sales.sale_user  WHERE client_no = '" + client_no + "'; ";
            pps = conn_.prepareStatement(_sql);
            ResultSet _rs = pps.executeQuery();
            while (_rs.next()) {
                String cert_no = _rs.getString("CERT_NO");    // 身份证号
//                client_no = "100120170729121953";
//                cert_no = "130283199310197614";
//                filePath = "C:\\Users\\ls\\Desktop\\94daa7c0-2ce9-11e8-bd45-00163e0e5886.json";  //  功能自测
                readFileByLines(filePath, voucher_no, client_no, cert_no);
                System.out.println(DateUtil.nowString() + " ==MoxieTrip_info2Hbase=files1====" + ++files + "===filepath===" + filePath);
            }
        }
    }


}
