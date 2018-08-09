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

/* 摩羯  社保 */
public class MoxieSecurity2Hbase {

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

            JSONObject basic_info = (JSONObject) jobj.get("basic_info");
            if (basic_info != null) {
                Set<String> keySet = basic_info.keySet();
                for (String key : keySet) {
                    String s = basic_info.getString(key);
                    s = s.substring(0, 1);
                    if ("{".equals(s)) {
                        JSONObject job = (JSONObject) basic_info.get(key);
                        insertJSONObject2Hbase("mojie_report_security_" + key, job, cert_no, voucher_no, client_no);
                    }
                }
            }

            JSONObject social_insurance_summary = (JSONObject) jobj.get("social_insurance_summary");
            if (social_insurance_summary != null) {
                Set<String> keySet = social_insurance_summary.keySet();
                for (String key : keySet) {
                    String s = social_insurance_summary.getString(key);
                    s = s.substring(0, 1);
                    if ("[".equals(s)) {
                        JSONArray jrr = social_insurance_summary.getJSONArray(key);
                        insertJSONArray2Hbase("mojie_report_security_" + key, jrr, cert_no, voucher_no, client_no);
                    }
                }
            }

            JSONObject medical_insurance_bill = (JSONObject) jobj.get("medical_insurance_bill");
            if (medical_insurance_bill != null) {
                Set<String> keySet = medical_insurance_bill.keySet();
                for (String key : keySet) {
                    String s = medical_insurance_bill.getString(key);
                    s = s.substring(0, 1);
                    if ("{".equals(s)) {
                        JSONObject job = (JSONObject) medical_insurance_bill.get(key);
                        Set<String> jobSet = job.keySet();
                        for (String k : jobSet) {
                            String s_ = job.getString(k);
                            s_ = s_.substring(0, 1);
                            if ("{".equals(s_)) {
                                JSONObject j = (JSONObject) job.get(k);
                                insertJSONObject2Hbase("mojie_report_security_"+k, j, cert_no, voucher_no, client_no);
                            }
                        }

                    }
                }
            }

            JSONObject medical_consumption_details = (JSONObject) jobj.get("medical_consumption_details");
            if (medical_consumption_details != null) {
                Set<String> keySet = medical_consumption_details.keySet();
                for (String key : keySet) {
                    String s = medical_consumption_details.getString(key);
                    s = s.substring(0, 1);
                    if ("{".equals(s)) {
                        JSONObject job = (JSONObject) medical_consumption_details.get(key);
                        Set<String> jobSet = job.keySet();
                        for (String k : jobSet) {
                            String s_ = job.getString(k);
                            s_ = s_.substring(0, 1);
                            if ("{".equals(s_)) {
                                JSONObject j = (JSONObject) job.get(k);
                                insertJSONObject2Hbase("mojie_report_security_" + k, j, cert_no, voucher_no, client_no);
                            } else if ("[".equals(s_)) {
                                JSONArray jrr = job.getJSONArray(k);
                                insertJSONArray2Hbase("mojie_report_security_" + k, jrr, cert_no, voucher_no, client_no);
                            }
                        }

                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(filePath);
        }
    }

    private static void insertJSONArray2Hbase(String tableName, JSONArray jrr, String cert_no, String voucher_no, String client_no) throws IOException {
        List<Map<String, String>> lists = new ArrayList<>();
        Table table = HBaseUtils.getHbaseTbale(tableName);

        int count = 0;
        for (Iterator iterator = jrr.iterator(); iterator.hasNext(); ) {
            HashMap<String, String> hashmap = new HashMap<>();
            JSONObject job = (JSONObject) iterator.next();
            Set<String> keySet = job.keySet();
            for (String key : keySet) {
                hashmap.put(key, job.getString(key));
            }
            hashmap.put("rowKey", client_no + "_" + cert_no + "_" + count++);
            hashmap.put("cert_no", cert_no);
            hashmap.put("voucher_no", voucher_no);
            hashmap.put("client_no", client_no);
            hashmap.put("loadtime", DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
            lists.add(hashmap);
            if (lists.size() % 100000 == 0) {
                HBaseUtils.insert(table, lists);
                lists.clear();
            }
        }

        HBaseUtils.insert(table, lists);
        table.close();
    }

    private static void insertJSONObject2Hbase(String tableName, JSONObject job, String cert_no, String voucher_no, String client_no) throws Exception {
        List<Map<String, String>> lists = new ArrayList<>();
        HashMap<String, String> hashmap = new HashMap<>();
        Set<String> keySet = job.keySet();
        for (String key : keySet) {
            hashmap.put(key, job.getString(key));
        }
        hashmap.put("rowKey", client_no + "_" + cert_no);
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
        String sql = "SELECT VOUCHER_NO,FILE_PATH FROM cdsp_hulu_access n WHERE QUERY_TYPE = 'MOXIE_SECURITY_REPORT' " +
                "AND n.GMT_CREATED BETWEEN '" + args[0] + "' AND '" + args[1] + "' ";
        PreparedStatement pps;
        try {
            pps = conn.prepareStatement(sql);
            ResultSet rs = pps.executeQuery();
            while (rs.next()) {
                String voucher_no = rs.getString("VOUCHER_NO");     // 授信编号
                String filePath = rs.getString("FILE_PATH");        // 文件路径
//                filePath = "D:\\tmp\\mojie_report_security.json";  //  功能自测
//                filePath = "D:\\workspace\\数据样例\\moxie_security_1229.json";  //  功能自测
                String sql_ = "SELECT client_no FROM bops.bops_loan_request r WHERE REQ_no = '" + voucher_no + "'; ";
                pps = conn_.prepareStatement(sql_);
                ResultSet rs_ = pps.executeQuery();
                getFiles(conn_, voucher_no, filePath, rs_);

                if (flag == 0) {
                    sql_ = "SELECT client_no FROM bops.bops_credit_request r WHERE credit_no = '" + voucher_no + "'; ";
                    pps = conn_.prepareStatement(sql_);
                    rs_ = pps.executeQuery();
                    getFiles_(conn_, voucher_no, filePath, rs_);
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
        System.out.println(DateUtil.nowString() + " MoxieSecurity2Hbase导入耗时为： " + (endtime - starttime));
    }

    static int flag = 0;

    private static void getFiles(Connection conn_, String voucher_no, String filePath, ResultSet rs_) throws SQLException {
        PreparedStatement pps;
        while (rs_.next()) {
            flag = 1;
            String client_no = rs_.getString("client_no");   // 客户编号
            String _sql = "SELECT CERT_NO FROM sales.sale_user  WHERE client_no = '" + client_no + "'; ";
            pps = conn_.prepareStatement(_sql);
            ResultSet _rs = pps.executeQuery();
            while (_rs.next()) {
                String cert_no = _rs.getString("CERT_NO");    // 身份证号
                readFileByLines(filePath, voucher_no, client_no, cert_no);
                System.out.println(DateUtil.nowString() + " ==MoxieSecurity2Hbase=files1====" + ++files + "===filepath===" + filePath);
            }
        }
    }

    private static void getFiles_(Connection conn_, String voucher_no, String filePath, ResultSet rs_) throws SQLException {
        PreparedStatement pps;
        while (rs_.next()) {
            String client_no = rs_.getString("client_no");   // 客户编号
            String _sql = "SELECT CERT_NO FROM sales.sale_user  WHERE client_no = '" + client_no + "'; ";
            pps = conn_.prepareStatement(_sql);
            ResultSet _rs = pps.executeQuery();
            while (_rs.next()) {
                String cert_no = _rs.getString("CERT_NO");    // 身份证号
                readFileByLines(filePath, voucher_no, client_no, cert_no);
                System.out.println(DateUtil.nowString() + " ==MoxieSecurity2Hbase=files1====" + ++files + "===filepath===" + filePath);
            }
        }
    }


}
