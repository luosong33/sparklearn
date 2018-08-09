package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class EmayBasic2Phoenix {

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
                    try {
                        JSONObject jobj = JSON.parseObject(tempString);
                        //  emay_basic
                        String local_mobile = jobj.getString("KEY");
                        String province = jobj.getString("PROVINCE");
                        String city = jobj.getString("CITY");
                        String cycle = jobj.getString("CYCLE");
                        String exists = jobj.getString("EXISTS");
                        String sql1 = "upsert  into \"emay_basic\" ( \"ID\", \"local_mobile\", \"province\", \"city\", \"cycle\", \"exists\", \"voucher_no\", " +
                                "\"client_no\", \"cert_no\", \"loadtime\") values (?,?,?,?,?,?,?,?,?,?) ";
                        Connection conn = GetConnection.getPhoenixConn();
                        PreparedStatement ps = null;
                        try {
                            ps = conn.prepareStatement(sql1);
                            ps.setString(1, cert_no);
                            ps.setString(2, local_mobile);
                            ps.setString(3, province);
                            ps.setString(4, city);
                            ps.setString(5, cycle);
                            ps.setString(6, exists);
                            ps.setString(7, voucher_no);
                            ps.setString(8, client_no);
                            ps.setString(9, cert_no);
                            ps.setString(10, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                            ps.addBatch();
                            ps.executeBatch();
                            conn.commit();
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                        JSONArray results = jobj.getJSONArray("RESULTS");
                        for (Iterator iterator = results.iterator(); iterator.hasNext(); ) {
                            JSONObject job = (JSONObject) iterator.next();
                            String type = job.getString("TYPE");
                            JSONArray data = job.getJSONArray("DATA");
                            if ("EMR002".equals(type)) {
                                insertEmay002(data, cert_no, voucher_no, client_no);
                            } else if ("EMR004".equals(type)) {
                                insertEmay004(data, cert_no, voucher_no, client_no);
                            } else if ("EMR007".equals(type)) {
                                insertEmay007(data, cert_no, voucher_no, client_no);
                            } else if ("EMR012".equals(type)) {
                                insertEmay012(data, cert_no, voucher_no, client_no);
                            }
                        }
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

    private static void insertEmay012(JSONArray emr012, String cert_no, String voucher_no, String client_no) {
        Connection conn = GetConnection.getPhoenixConn();
        PreparedStatement stmt = null;
        String sql = "upsert into \"emay_emr012\" ( \"ID\", \"emr012_platform\", \"emr012_counts\", \"emr012_money\", \"emr012_d_time\", \"voucher_no\", " +
                "\"client_no\", \"cert_no\", \"loadtime\" ) values (?,?,?,?,?,?,?,?,?) ";
        try {
            stmt = conn.prepareStatement(sql);
            int count = 0;
            for (Iterator iterator = emr012.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                String platform = job.getString("PLATFORM");
                String counts = job.getString("COUNTS");
                String money = job.getString("MONEY");
                String d_time = job.getString("D_TIME");
                try {
                    stmt.setString(1, cert_no + "_" + count);
                    stmt.setString(2, platform);
                    stmt.setString(3, counts);
                    stmt.setString(4, money);
                    stmt.setString(5, d_time);
                    stmt.setString(6, voucher_no);
                    stmt.setString(7, client_no);
                    stmt.setString(8, cert_no);
                    stmt.setString(9, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                    stmt.addBatch();
                    count++;
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                if (count % 100 == 0) {
                    try {
                        stmt.executeBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            stmt.executeBatch();
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertEmay007(JSONArray emr007, String cert_no, String voucher_no, String client_no) {
        Connection conn = GetConnection.getPhoenixConn();
        PreparedStatement stmt = null;
        String sql = "upsert into \"emay_emr007\" ( \"ID\", \"emr007_p_type\", \"emr007_platformcode\", \"emr007_loanlenderstime\", \"emr007_loanlendersamount\", " +
                "\"voucher_no\", \"client_no\", \"cert_no\", \"loadtime\" ) values (?,?,?,?,?,?,?,?,?) ";
        try {
            stmt = conn.prepareStatement(sql);
            int count = 0;
            for (Iterator iterator = emr007.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                String p_type = job.getString("P_TYPE");
                String platformcode = job.getString("PLATFORMCODE");
                String loanlenderstime = job.getString("LOANLENDERSTIME");
                String loanlendersamount = job.getString("LOANLENDERSAMOUNT");
                try {
                    stmt.setString(1, cert_no + "_" + count);
                    stmt.setString(2, p_type);
                    stmt.setString(3, platformcode);
                    stmt.setString(4, loanlenderstime);
                    stmt.setString(5, loanlendersamount);
                    stmt.setString(6, voucher_no);
                    stmt.setString(7, client_no);
                    stmt.setString(8, cert_no);
                    stmt.setString(9, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                    stmt.addBatch();
                    count++;
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                if (count % 100 == 0) {
                    try {
                        stmt.executeBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            stmt.executeBatch();
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertEmay004(JSONArray emr004, String cert_no, String voucher_no, String client_no) {
        Connection conn = GetConnection.getPhoenixConn();
        PreparedStatement stmt = null;
        String sql = "upsert into \"emay_emr004\" ( \"ID\", \"emr004_p_type\", \"emr004_platformcode\", \"emr004_applicationtime\", \"emr004_applicationamount\", " +
                "\"emr004_applicationresult\", \"voucher_no\", \"client_no\", \"cert_no\", \"loadtime\")values(?,?,?,?,?,?,?,?,?,?) ";
        try {
            stmt = conn.prepareStatement(sql);
            int count = 0;
            for (Iterator iterator = emr004.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                String p_type = job.getString("P_TYPE");
                String platformcode = job.getString("PLATFORMCODE");
                String applicationtime = job.getString("APPLICATIONTIME");
                String applicationamount = job.getString("APPLICATIONAMOUNT");
                String applicationresult = job.getString("APPLICATIONRESULT");
                try {
                    stmt.setString(1, cert_no + "_" + count);
                    stmt.setString(2, p_type);
                    stmt.setString(3, platformcode);
                    stmt.setString(4, applicationtime);
                    stmt.setString(5, applicationamount);
                    stmt.setString(6, applicationresult);
                    stmt.setString(7, voucher_no);
                    stmt.setString(8, client_no);
                    stmt.setString(9, cert_no);
                    stmt.setString(10, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                    stmt.addBatch();
                    count++;
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                if (count % 100 == 0) {
                    try {
                        stmt.executeBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            stmt.executeBatch();
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertEmay002(JSONArray emr002, String cert_no, String voucher_no, String client_no) {
        Connection conn = GetConnection.getPhoenixConn();
        PreparedStatement stmt = null;
        String sql = "upsert into \"emay_emr002\" ( \"ID\", \"emr002_p_type\", \"emr002_platformcode\", \"emr002_registertime\", \"voucher_no\", \"client_no\", " +
                "\"cert_no\", \"loadtime\") values (?,?,?,?,?,?,?,?) ";
        try {
            stmt = conn.prepareStatement(sql);
            int count = 0;
            for (Iterator iterator = emr002.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                String p_type = job.getString("P_TYPE");
                String platformcode = job.getString("PLATFORMCODE");
                String registertime = job.getString("REGISTERTIME");
                try {
                    stmt.setString(1, cert_no + "_" + count);
                    stmt.setString(2, p_type);
                    stmt.setString(3, platformcode);
                    stmt.setString(4, registertime);
                    stmt.setString(5, voucher_no);
                    stmt.setString(6, client_no);
                    stmt.setString(7, cert_no);
                    stmt.setString(8, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                    stmt.addBatch();
                    count++;
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                if (count % 100 == 0) {
                    try {
                        stmt.executeBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            stmt.executeBatch();
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        int files = 0;
        Connection conn = GetConnection.getConn_Cdsp_Riskcore();
        Connection conn_ = GetConnection.getConn_Bops_Sales();
        String sql = "";
        sql = "SELECT n.ex_serial,a.FILE_PATH FROM cdsp.cdsp_network_crawl_task n,cdsp.cdsp_allwin_credit a " +
                "WHERE n.api_code = \"EMAY_LOAN_CREDIT_ACCESS_SUMMIT\" AND n.GMT_CREATED BETWEEN '" + args[0] + "' AND '" + args[1] + "' " + //   2017-11-09 21:12:54
                "AND a.TASK_NO = n.TASK_NO ";
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String voucher_no = rs.getString("ex_serial");     // 授信编号
                String filePath = rs.getString("FILE_PATH");        // 文件路径
                String sql_ = "SELECT client_no FROM bops.bops_credit_request r WHERE CREDIT_NO = '" + voucher_no + "'; ";
                pstmt = conn_.prepareStatement(sql_);
                ResultSet rs_ = pstmt.executeQuery();
                while (rs_.next()) {
                    String client_no = rs_.getString("CLIENT_NO");   // 客户编号
                    String _sql = "SELECT u.CERT_NO FROM sales.sale_user u WHERE client_no = '" + client_no + "'; ";
                    pstmt = conn_.prepareStatement(_sql);
                    ResultSet _rs = pstmt.executeQuery();
                    while (_rs.next()) {
                        String cert_no = _rs.getString("CERT_NO");    // 身份证号
//                        filePath = "D:\\tmp\\TASK20171102433199.txt";
                        readFileByLines(filePath, voucher_no, client_no, cert_no);
                        System.out.println(DateUtil.nowString() + " ==dianshang=files====" + ++files);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
            conn_.close();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " emayBasic2Phoenix导入耗时为： " + (endtime - starttime));
    }


}
