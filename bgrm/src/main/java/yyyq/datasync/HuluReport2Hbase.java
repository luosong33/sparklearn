package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.*;

import java.io.*;
import java.sql.*;
import java.util.Iterator;

public class HuluReport2Hbase {

    //  通讯详单入库
    public static void insert_contact_list(String voucherNo, String mobile_local, JSONArray contact_listJarr) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"hulu_report_contact\" (\"ID\", \"contact_noon\",\"phone_num_loc\",\"contact_3m\",\"contact_1m\",\"contact_1w\",\"p_relation\",\"phone_num\",\"contact_name\",\"call_in_cnt\",\"call_out_cnt\",\"call_out_len\",\"contact_holiday\",\"needs_type\",\"contact_weekday\",\"contact_afternoon\",\"call_len\",\"contact_early_morning\",\"contact_night\",\"contact_3m_plus\",\"call_cnt\",\"call_in_len\",\"contact_all_day\",\"contact_morning\",\"contact_weekend\",\"mobile_local\",\"voucher_no\",\"loadtime\")values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        int count = 0;
        for (Iterator iterator = contact_listJarr.iterator(); iterator.hasNext(); ) {
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

    //  通讯详单组装插入语句
    private static void setStmt(String voucherNo, String mobile_local, PreparedStatement stmt, JSONObject job) throws SQLException {
        stmt.setString(1, voucherNo + "_" + mobile_local + "_" + String.valueOf(job.get("phone_num")));
        stmt.setString(2, String.valueOf(job.getString("contact_noon")));
        stmt.setString(3, String.valueOf(job.getString("phone_num_loc")));
        stmt.setString(4, String.valueOf(job.getString("contact_3m")));
        stmt.setString(5, String.valueOf(job.getString("contact_1m")));
        stmt.setString(6, String.valueOf(job.getString("contact_1w")));
        stmt.setString(7, String.valueOf(job.getString("p_relation")));
        stmt.setString(8, String.valueOf(job.getString("phone_num")));
        stmt.setString(9, String.valueOf(job.getString("contact_name")));
        stmt.setString(10, String.valueOf(job.getString("call_in_cnt")));
        stmt.setString(11, String.valueOf(job.getString("call_out_cnt")));
        stmt.setString(12, String.valueOf(job.getString("call_out_len")));
        stmt.setString(13, String.valueOf(job.getString("contact_holiday")));
        stmt.setString(14, String.valueOf(job.getString("needs_type")));
        stmt.setString(15, String.valueOf(job.getString("contact_weekday")));
        stmt.setString(16, String.valueOf(job.getString("contact_afternoon")));
        stmt.setString(17, String.valueOf(job.getString("call_len")));
        stmt.setString(18, String.valueOf(job.getString("contact_early_morning")));
        stmt.setString(19, String.valueOf(job.getString("contact_night")));
        stmt.setString(20, String.valueOf(job.getString("contact_3m_plus")));
        stmt.setString(21, String.valueOf(job.getString("call_cnt")));
        stmt.setString(22, String.valueOf(job.getString("call_in_len")));
        stmt.setString(23, String.valueOf(job.getString("contact_all_day")));
        stmt.setString(24, String.valueOf(job.getString("contact_morning")));
        stmt.setString(25, String.valueOf(job.getString("contact_weekend")));
        stmt.setString(26, mobile_local);
        stmt.setString(27, voucherNo);
        stmt.setString(28, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
    }

    //  行为检测入库
    public static void insert_behavior_check(String voucherNo, String mobile_local, JSONArray behavior_checkJarr) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"hulu_report_behavior_check\" (\"ID\", \"behavior_check_1_check_point\" ,\"behavior_check_1_score\" ,\"behavior_check_1_result\" ,\"behavior_check_1_evidence\" ,\"behavior_check_2_check_point\" ,\"behavior_check_2_score\" ,\"behavior_check_2_result\" ,\"behavior_check_2_evidence\" ,\"behavior_check_3_check_point\" ,\"behavior_check_3_score\" ,\"behavior_check_3_result\" ,\"behavior_check_3_evidence\" ,\"behavior_check_4_check_point\" ,\"behavior_check_4_score\" ,\"behavior_check_4_result\" ,\"behavior_check_4_evidence\" ,\"behavior_check_5_check_point\" ,\"behavior_check_5_score\" ,\"behavior_check_5_result\" ,\"behavior_check_5_evidence\" ,\"behavior_check_6_check_point\" ,\"behavior_check_6_score\" ,\"behavior_check_6_result\" ,\"behavior_check_6_evidence\" ,\"behavior_check_7_check_point\" ,\"behavior_check_7_score\" ,\"behavior_check_7_result\" ,\"behavior_check_7_evidence\" ,\"behavior_check_8_check_point\" ,\"behavior_check_8_score\" ,\"behavior_check_8_result\" ,\"behavior_check_8_evidence\" ,\"behavior_check_9_check_point\" ,\"behavior_check_9_score\" ,\"behavior_check_9_result\" ,\"behavior_check_9_evidence\" ,\"behavior_check_10_check_point\" ,\"behavior_check_10_score\" ,\"behavior_check_10_result\" ,\"behavior_check_10_evidence\" ,\"behavior_check_11_check_point\" ,\"behavior_check_11_score\" ,\"behavior_check_11_result\" ,\"behavior_check_11_evidence\" ,\"behavior_check_12_check_point\" ,\"behavior_check_12_score\" ,\"behavior_check_12_result\" ,\"behavior_check_12_evidence\" ,\"behavior_check_13_check_point\" ,\"behavior_check_13_score\" ,\"behavior_check_13_result\" ,\"behavior_check_13_evidence\" ,\"behavior_check_14_check_point\" ,\"behavior_check_14_score\" ,\"behavior_check_14_result\" ,\"behavior_check_14_evidence\" ,\"behavior_check_15_check_point\" ,\"behavior_check_15_score\" ,\"behavior_check_15_result\" ,\"behavior_check_15_evidence\" ,\"behavior_check_16_check_point\" ,\"behavior_check_16_score\" ,\"behavior_check_16_result\" ,\"behavior_check_16_evidence\" ,\"behavior_check_17_check_point\" ,\"behavior_check_17_score\" ,\"behavior_check_17_result\" ,\"behavior_check_17_evidence\" ,\"behavior_check_18_check_point\" ,\"behavior_check_18_score\" ,\"behavior_check_18_result\" ,\"behavior_check_18_evidence\" ,\"behavior_check_19_check_point\" ,\"behavior_check_19_score\" ,\"behavior_check_19_result\" ,\"behavior_check_19_evidence\" ,\"behavior_check_20_check_point\" ,\"behavior_check_20_score\" ,\"behavior_check_20_result\" ,\"behavior_check_20_evidence\" ,\"behavior_check_21_check_point\" ,\"behavior_check_21_score\" ,\"behavior_check_21_result\" ,\"behavior_check_21_evidence\" ,\"behavior_check_22_check_point\" ,\"behavior_check_22_score\" ,\"behavior_check_22_result\" ,\"behavior_check_22_evidence\" ,\"behavior_check_23_check_point\" ,\"behavior_check_23_score\" ,\"behavior_check_23_result\" ,\"behavior_check_23_evidence\" ,\"behavior_check_24_check_point\" ,\"behavior_check_24_score\" ,\"behavior_check_24_result\" ,\"behavior_check_24_evidence\" ,\"behavior_check_25_check_point\" ,\"behavior_check_25_score\" ,\"behavior_check_25_result\" ,\"behavior_check_25_evidence\" ,\"behavior_check_26_check_point\" ,\"behavior_check_26_score\" ,\"behavior_check_26_result\" ,\"behavior_check_26_evidence\" ,\"behavior_check_27_check_point\" ,\"behavior_check_27_score\" ,\"behavior_check_27_result\" ,\"behavior_check_27_evidence\" ,\"behavior_check_28_check_point\" ,\"behavior_check_28_score\" ,\"behavior_check_28_result\" ,\"behavior_check_28_evidence\" ,\"voucher_no\",\"loadtime\")values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            stmt.setString(1, voucherNo + "_" + mobile_local);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        try {
            int i = 2;
            for (Iterator iterator = behavior_checkJarr.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                stmt.setString(i, String.valueOf(job.getString("check_point")));
                stmt.setString(++i, String.valueOf(job.getString("score")));
                stmt.setString(++i, String.valueOf(job.getString("result")));
                stmt.setString(++i, String.valueOf(job.getString("evidence")));
                i++;
            }
            stmt.setString(114, voucherNo);
            stmt.setString(115, mobile_local);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            /*stmt.executeUpdate();
            con.commit();
            con.close();*/
            stmt.addBatch();
            stmt.executeBatch();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  葫芦联系人区域汇总入库
    public static void insert_contact_region(String voucherNo, String mobile_local, JSONArray contact_regionJarr) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"hulu_report_contact_region\" (\"ID\", \"region_loc\",\"region_uniq_num_cnt\",\"region_call_out_time\",\"region_avg_call_in_time\",\"region_call_in_time\",\"region_call_out_cnt\",\"region_avg_call_out_time\",\"region_call_in_cnt_pct\",\"region_call_in_time_pct\",\"region_call_in_cnt\",\"region_call_out_time_pct\",\"region_call_out_cnt_pct\",\"voucher_no\",\"loadtime\")values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        int count = 0;
        try {
            int i = 0;
            for (Iterator iterator = contact_regionJarr.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                setStmt_contact_region(voucherNo, mobile_local, stmt, job, i);
                stmt.addBatch();
                count++;
                i++;
            }

            if (count % 1000 == 0) {
                stmt.executeBatch();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            stmt.executeBatch();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  联系人区域汇总插入语句
    private static void setStmt_contact_region(String voucherNo, String mobile_local, PreparedStatement stmt, JSONObject job, int i) throws SQLException {
        SnowflakeIdGenerator idWorker = new SnowflakeIdGenerator(0, 0);
        long id = idWorker.nextId();
        stmt.setString(1, voucherNo + "_" + mobile_local+"_"+i);
        stmt.setString(2, String.valueOf(job.getString("region_loc")));
        stmt.setString(3, String.valueOf(job.getString("region_uniq_num_cnt")));
        stmt.setString(4, String.valueOf(job.getString("region_call_out_time")));
        stmt.setString(5, String.valueOf(job.getString("region_avg_call_in_time")));
        stmt.setString(6, String.valueOf(job.getString("region_call_in_time")));
        stmt.setString(7, String.valueOf(job.getString("region_call_out_cnt")));
        stmt.setString(8, String.valueOf(job.getString("region_avg_call_out_time")));
        stmt.setString(9, String.valueOf(job.getString("region_call_in_cnt_pct")));
        stmt.setString(10, String.valueOf(job.getString("region_call_in_time_pct")));
        stmt.setString(11, String.valueOf(job.getString("region_call_in_cnt")));
        stmt.setString(12, String.valueOf(job.getString("region_call_out_time_pct")));
        stmt.setString(13, String.valueOf(job.getString("region_call_out_cnt_pct")));
        stmt.setString(14, String.valueOf(job.getString("voucher_no")));
        stmt.setString(15, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
    }

    //  运营商数据整理入库
    public static void insert_cell_behavior(String voucherNo, String mobile_local, JSONArray cell_behaviorJarr) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"hulu_report_cell_behavior\" (\"ID\", \"phone_num_local\" ,\"sms_cnt\" ,\"cell_phone_num\" ,\"net_flow\" ,\"total_amount\" ,\"call_out_time\" ,\"cell_mth\" ,\"cell_loc\" ,\"call_cnt\" ,\"cell_operator_zh\" ,\"call_out_cnt\" ,\"cell_operator\" ,\"call_in_time\" ,\"call_in_cnt\" ,\"voucher_no\",\"loadtime\")values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        int count = 0;
        try {
            int i = 0;
            for (Iterator iterator = cell_behaviorJarr.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                setStmt_cell_behavior(voucherNo, mobile_local, stmt, job, i);
                stmt.addBatch();
                count++;
                i++;
            }

            if (count % 1000 == 0) {
                stmt.executeBatch();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            stmt.executeBatch();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  运营商数据整理语句
    private static void setStmt_cell_behavior(String voucherNo, String mobile_local, PreparedStatement stmt, JSONObject job, int i) throws SQLException {
        SnowflakeIdGenerator idWorker = new SnowflakeIdGenerator(0, 0);
        long id = idWorker.nextId();
        try {
            stmt.setString(1, voucherNo + "_" + mobile_local+"_"+i);
            stmt.setString(2, mobile_local);
            stmt.setString(3, String.valueOf(job.getString("sms_cnt")));
            stmt.setString(4, String.valueOf(job.getString("cell_phone_num")));
            stmt.setString(5, String.valueOf(job.getString("net_flow")));
            stmt.setString(6, String.valueOf(job.getString("total_amount")));
            stmt.setString(7, String.valueOf(job.getString("call_out_time")));
            stmt.setString(8, String.valueOf(job.getString("cell_mth")));
            stmt.setString(9, String.valueOf(job.getString("cell_loc")));
            stmt.setString(10, String.valueOf(job.getString("call_cnt")));
            stmt.setString(11, String.valueOf(job.getString("cell_operator_zh")));
            stmt.setString(12, String.valueOf(job.getString("call_out_cnt")));
            stmt.setString(13, String.valueOf(job.getString("cell_operator")));
            stmt.setString(14, String.valueOf(job.getString("call_in_time")));
            stmt.setString(15, String.valueOf(job.getString("call_in_cnt")));
            stmt.setString(16, voucherNo);
            stmt.setString(17, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  读文件解析
    public static void readFileByLines(String voucherNo, String fileName) {
//        System.out.println("========start========" + fileName);
        File file = null;
        try {
//            SshUtil.getContext("192.168.15.196","root","yinghuo#123", fileName);
//            file = new File("d:/tmp/b.txt");
//            file = new File("d:/tmp/hulu_report_1108.txt");
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
                        String mobile = "";
                        //  葫芦
                        JSONObject pushDataJson = (JSONObject) jobj.get("push_data");
                        mobile = (String) jobj.get("cell_phone_number");  //  取本人手机号

                        JSONArray contact_listJarr = pushDataJson.getJSONArray("contact_list");  //  数组
                        insert_contact_list(voucherNo, mobile, contact_listJarr);

                        JSONArray behavior_checkJarr = pushDataJson.getJSONArray("behavior_check");  //  数组
                        insert_behavior_check(voucherNo, mobile, behavior_checkJarr);

                        JSONArray contact_regionJarr = pushDataJson.getJSONArray("contact_region");  //  数组
                        insert_contact_region(voucherNo, mobile, contact_regionJarr);

                        JSONArray cell_behavior = pushDataJson.getJSONArray("cell_behavior");  //  数组
                        JSONObject o = (JSONObject)cell_behavior.get(0);
                        JSONArray behavior = o.getJSONArray("behavior");  //  数组
                        insert_cell_behavior(voucherNo, mobile, behavior);

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
                + "and GMT_CREATED >= '" + args[0] + "' " + "and GMT_CREATED < '" + args[1] + "' ";
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String voucher = rs.getString("VOUCHER_NO");  //  葫芦、摩羯
                String filePath = rs.getString("FILE_PATH");
                readFileByLines(voucher, filePath);
                System.out.println(DateUtil.nowString() + " ==Call=files=" + ++files + "==filepath=" + filePath);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " 耗时为： " + (endtime - starttime));
    }
}