package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;
import yyyq.util.RowKeyUtil;

import java.io.*;
import java.sql.*;
import java.util.Iterator;

/* 摩羯运营商报告入库 */
public class CallRecordMoxie2Hbase {
    private static String nowDate = DateUtil.nowString();

    //  读文件获取内容
    public static void readFileByLines(String voucherNo, String fileName) {
//        System.out.println("========start========" + fileName);
        File file = null;
        try {
//            file = new File("/tmp/nfs/cdsp/hulu/report/20170729/122c1a4db5ce429eae32c3f0435ce480.txt");
            file = new File(fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (file != null) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
                String tempString = "";
                while ((tempString = reader.readLine()) != null) {
                    analysis(tempString, voucherNo);
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

    //  解析内容
    public static void analysis(String content, String voucherNo) {
        String idWorker = RowKeyUtil.getIdWorker(5);
        try {
            JSONObject jobj = JSON.parseObject(content);
            String mobile = "";
            //  摩羯
            JSONArray cell_phoneArr = jobj.getJSONArray("cell_phone");  //  取本人手机号
            for (Iterator iterator = cell_phoneArr.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                    if ("mobile".equals(job.get("key"))) {
                        mobile = (String) job.get("value");
                }
            }
            //  摩羯通讯详单
            JSONArray call_contact_detail = jobj.getJSONArray("call_contact_detail");
            String SQL = "upsert  into \"capricorn_report_call_contact_detail\" (\"ROW\",\"city\", \"p_relation\", \"mobile_answer\", \"group_name\", \"company_name\", \"call_cnt_1w\", \"call_cnt_1m\", \"call_cnt_3m\", \"call_cnt_6m\", \"call_time_3m\", \"call_time_6m\", \"dial_cnt_3m\", \"dial_cnt_6m\", \"dial_time_3m\", \"dial_time_6m\", \"dialed_cnt_3m\", \"dialed_cnt_6m\", \"dialed_time_3m\", \"dialed_time_6m\", \"call_cnt_morning_3m\", \"call_cnt_morning_6m\", \"call_cnt_noon_3m\", \"call_cnt_noon_6m\", \"call_cnt_afternoon_3m\", \"call_cnt_afternoon_6m\", \"call_cnt_evening_3m\", \"call_cnt_evening_6m\", \"call_cnt_night_3m\", \"call_cnt_night_6m\", \"call_cnt_weekday_3m\", \"call_cnt_weekday_6m\", \"call_cnt_weekend_3m\", \"call_cnt_weekend_6m\", \"call_cnt_holiday_3m\", \"call_cnt_holiday_6m\", \"call_if_whole_day_3m\", \"call_if_whole_day_6m\", \"trans_start\", \"trans_end\", \"mobile_local\", \"voucher_no\",\"load_time\") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            insertBach(SQL, call_contact_detail, voucherNo, mobile, "call_contact_detail",idWorker);
            //  手机号基础信息表
            JSONArray cell_phone = jobj.getJSONArray("cell_phone");
            String SQL1 = "upsert  into \"capricorn_report_cell_phone\" ( \"ROW\", \"mobile\", \"carrier_name\", \"carrier_idcard\", \"reg_time\", \"in_time\", \"email\", \"address\", \"reliability\", \"phone_attribution\", \"live_address\", \"available_balance\", \"package_name\", \"bill_certification_day\",\"load_time\")values( ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?,?)";
            insert(SQL1, cell_phone, voucherNo, mobile, "cell_phone",idWorker);
            //  消费详单表
            JSONArray consumption_detail = jobj.getJSONArray("consumption_detail");
            String SQL2 = "upsert  into \"capricorn_report_consumption_detail\" ( \"ROW\",  \"app_point\", \"app_point_zh\", \"item_1m\", \"item_3m\", \"item_6m\", \"avg_item_3m\", \"avg_item_6m\", \"load_time\"  ) values ( ?,?,?,?,?, ?,?,?,? )";
            insertBach(SQL2, consumption_detail, voucherNo, mobile, "consumption_detail",idWorker);
            //  行为监测表
            JSONArray behavior_check = jobj.getJSONArray("behavior_check");
            String SQL3 = "upsert  into \"capricorn_report_behavior_check\" ( \"ROW\",\"regular_circle\",\"phone_used_time\",\"phone_silent\",\"phone_power_off\",\"contact_each_other\",\"contact_macao\",\"contact_110\",\"contact_120\",\"contact_lawyer\",\"contact_court\",\"contact_loan\",\"contact_bank\",\"contact_credit_card\",\"contact_collection\",\"contact_night\",\"dwell_used_time\",\"ebusiness_info\",\"person_ebusiness_info\",\"virtual_buying\",\"lottery_buying\",\"person_addr_changed\",\"school_status\",\"education_info\",\"work_addr_info\",\"live_addr_info\",\"school_addr_info\",\"phone_call\",\"load_time\" ) values ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?)";
            insert(SQL3, behavior_check, voucherNo, mobile, "behavior_check",idWorker);
            //  朋友圈联系人数量表
            JSONObject friend_circle = (JSONObject) jobj.get("friend_circle");
            JSONArray summary = friend_circle.getJSONArray("summary");
            String SQL4 = "upsert  into \"capricorn_report_friend_circle_summary\" ( \"ROW\", \"friend_num_3m\",\"good_friend_num_3m\",\"friend_city_center_3m\",\"is_city_match_friend_city_center_3m\",\"inter_peer_num_3m\",\"friend_num_6m\",\"good_friend_num_6m\",\"friend_city_center_6m\",\"is_city_match_friend_city_center_6m\",\"inter_peer_num_6m\",\"load_time\") values ( ?,?,?,?,?, ?,?,?,?,?,?,?)";
            insert(SQL4, summary, voucherNo, mobile, "summary",idWorker);
            //  朋友圈联系人数量表
            JSONArray contact_region = jobj.getJSONArray("contact_region");
            String SQL5 = "upsert  into \"capricorn_report_contact_region\" ( \"ROW\", \"region_loc\",\"region_uniq_num_cnt\",\"region_call_cnt\",\"region_call_time\",\"region_dial_cnt\",\"region_dial_time\",\"region_dialed_cnt\",\"region_dialed_time\",\"region_avg_dial_time\",\"region_avg_dialed_time\",\"region_dial_cnt_pct\",\"region_dial_time_pct\",\"region_dialed_cnt_pct\",\"region_dialed_time_pct\",\"contact_region_3m\",\"contact_region_6m\",\"load_time\")values( ?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,? ,?,?,?)";
            insertBach_arr2arr(SQL5, contact_region, voucherNo, mobile, "contact_region", idWorker);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量入库
     */
    public static void insertBach(String SQL, JSONArray jarr, String voucherNo, String mobile_local, String flag, String idWorker) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement(SQL);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        int count = 0;
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            try {
                if ("call_contact_detail".equals(flag)) {callContactDetail(voucherNo, mobile_local, stmt, job, idWorker);}
                else if ("consumption_detail".equals(flag)) {consumptionDetail(voucherNo, mobile_local, stmt, job, idWorker);}
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

    /**
     * 批量入库
     */
    public static void insertBach_arr2arr(String SQL, JSONArray jarr, String voucherNo, String mobile_local, String flag, String idWorker) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement(SQL);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        int count = 0;
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();

            String key = (String) job.get("key");
            String contact_region_3m = "";
            String contact_region_6m = "";
            if ("contact_region_3m".equals(key)){
                contact_region_3m = "1";
                contact_region_6m = "0";
            }else {
                contact_region_3m = "0";
                contact_region_6m = "1";
            }
            JSONArray jrr_ = null;
            if ("contact_region".equals(flag)) {jrr_ = job.getJSONArray("region_list");}
            for (Iterator iterator_ = jrr_.iterator(); iterator_.hasNext(); ) {
                JSONObject job_ = (JSONObject) iterator_.next();
                job_.put("contact_region_3m",contact_region_3m);
                job_.put("contact_region_6m",contact_region_6m);
                try {
                    if ("contact_region".equals(flag)) {contactRegion(voucherNo, mobile_local, stmt, job_, idWorker);}
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
        }

        try {
            stmt.executeBatch();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 入库
     */
    public static void insert(String SQL, JSONArray jarr, String voucherNo, String mobile_local, String flag, String idWorker) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement(SQL);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        try {
            if ("cell_phone".equals(flag)) {cellPhone(voucherNo, mobile_local, stmt, jarr, idWorker);}
            else if ("behavior_check".equals(flag)){behaviorCheck(voucherNo, mobile_local, stmt, jarr, idWorker);}
            else if ("summary".equals(flag)){summary(voucherNo, mobile_local, stmt, jarr, idWorker);}
            stmt.executeUpdate();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  通讯详单组装插入语句
    private static void callContactDetail(String voucherNo, String mobile_local, PreparedStatement stmt, JSONObject job, String idWorker) throws SQLException {
        //  注意通讯录唯一性    本人+对方
        stmt.setString(1, mobile_local + "_" + voucherNo + "_" + idWorker + "_" + String.valueOf(job.get("peer_num")));
        stmt.setString(2, String.valueOf(job.get("city")));
        stmt.setString(3, String.valueOf(job.get("p_relation")));
        stmt.setString(4, String.valueOf(job.get("peer_num")));
        stmt.setString(5, String.valueOf(job.get("group_name")));
        stmt.setString(6, String.valueOf(job.get("company_name")));
        stmt.setString(7, String.valueOf(job.get("call_cnt_1w")));
        stmt.setString(8, String.valueOf(job.get("call_cnt_1m")));
        stmt.setString(9, String.valueOf(job.get("call_cnt_3m")));
        stmt.setString(10, String.valueOf(job.get("call_cnt_6m")));
        stmt.setString(11, String.valueOf(job.get("call_time_3m")));
        stmt.setString(12, String.valueOf(job.get("call_time_6m")));
        stmt.setString(13, String.valueOf(job.get("dial_cnt_3m")));
        stmt.setString(14, String.valueOf(job.get("dial_cnt_6m")));
        stmt.setString(15, String.valueOf(job.get("dial_time_3m")));
        stmt.setString(16, String.valueOf(job.get("dial_time_6m")));
        stmt.setString(17, String.valueOf(job.get("dialed_cnt_3m")));
        stmt.setString(18, String.valueOf(job.get("dialed_cnt_6m")));
        stmt.setString(19, String.valueOf(job.get("dialed_time_3m")));
        stmt.setString(20, String.valueOf(job.get("dialed_time_6m")));
        stmt.setString(21, String.valueOf(job.get("call_cnt_morning_3m")));
        stmt.setString(22, String.valueOf(job.get("call_cnt_morning_6m")));
        stmt.setString(23, String.valueOf(job.get("call_cnt_noon_3m")));
        stmt.setString(24, String.valueOf(job.get("call_cnt_noon_6m")));
        stmt.setString(25, String.valueOf(job.get("call_cnt_afternoon_3m")));
        stmt.setString(26, String.valueOf(job.get("call_cnt_afternoon_6m")));
        stmt.setString(27, String.valueOf(job.get("call_cnt_evening_3m")));
        stmt.setString(28, String.valueOf(job.get("call_cnt_evening_6m")));
        stmt.setString(29, String.valueOf(job.get("call_cnt_night_3m")));
        stmt.setString(30, String.valueOf(job.get("call_cnt_night_6m")));
        stmt.setString(31, String.valueOf(job.get("call_cnt_weekday_3m")));
        stmt.setString(32, String.valueOf(job.get("call_cnt_weekday_6m")));
        stmt.setString(33, String.valueOf(job.get("call_cnt_weekend_3m")));
        stmt.setString(34, String.valueOf(job.get("call_cnt_weekend_6m")));
        stmt.setString(35, String.valueOf(job.get("call_cnt_holiday_3m")));
        stmt.setString(36, String.valueOf(job.get("call_cnt_holiday_6m")));
        stmt.setString(37, String.valueOf(job.get("call_if_whole_day_3m")));
        stmt.setString(38, String.valueOf(job.get("call_if_whole_day_6m")));
        stmt.setString(39, String.valueOf(job.get("trans_start")));
        stmt.setString(40, String.valueOf(job.get("trans_end")));
        stmt.setString(41, mobile_local);
        stmt.setString(42, voucherNo);
        stmt.setString(43, String.valueOf(nowDate));
    }

    //  消费详单表
    private static void consumptionDetail(String voucherNo, String mobile_local, PreparedStatement stmt, JSONObject job, String idWorker) throws SQLException {
        stmt.setString(1, mobile_local + "_" + voucherNo + "_" + idWorker);
        JSONObject item = (JSONObject) job.get("item");
        stmt.setString(2, String.valueOf(job.get("app_point")));
        stmt.setString(3, String.valueOf(job.get("app_point_zh")));
        stmt.setString(4, String.valueOf(item.get("item_1m")));
        stmt.setString(5, String.valueOf(item.get("item_3m")));
        stmt.setString(6, String.valueOf(item.get("item_6m")));
        stmt.setString(7, String.valueOf(item.get("avg_item_3m")));
        stmt.setString(8, String.valueOf(item.get("avg_item_6m")));
        stmt.setString(9, String.valueOf(nowDate));
    }

    //  联系人区域汇总
    private static void contactRegion(String voucherNo, String mobile_local, PreparedStatement stmt, JSONObject job, String idWorker) throws SQLException {
        stmt.setString(1, mobile_local + "_" + voucherNo + "_" + idWorker);
        stmt.setString(2, String.valueOf(job.get("region_loc")));
        stmt.setString(3, String.valueOf(job.get("region_uniq_num_cnt")));
        stmt.setString(4, String.valueOf(job.get("region_call_cnt")));
        stmt.setString(5, String.valueOf(job.get("region_call_time")));
        stmt.setString(6, String.valueOf(job.get("region_dial_cnt")));
        stmt.setString(7, String.valueOf(job.get("region_dial_time")));
        stmt.setString(8, String.valueOf(job.get("region_dialed_cnt")));
        stmt.setString(9, String.valueOf(job.get("region_dialed_time")));
        stmt.setString(10, String.valueOf(job.get("region_avg_dial_time")));
        stmt.setString(11, String.valueOf(job.get("region_avg_dialed_time")));
        stmt.setString(12, String.valueOf(job.get("region_dial_cnt_pct")));
        stmt.setString(13, String.valueOf(job.get("region_dial_time_pct")));
        stmt.setString(14, String.valueOf(job.get("region_dialed_cnt_pct")));
        stmt.setString(15, String.valueOf(job.get("region_dialed_time_pct")));
        stmt.setString(16,String.valueOf(job.get("contact_region_3m")));
        stmt.setString(17,String.valueOf(job.get("contact_region_6m")));
        stmt.setString(18, String.valueOf(nowDate));
    }

    //  组装插入语句
    private static void cellPhone(String voucherNo, String mobile_local, PreparedStatement stmt, JSONArray jrr, String idWorker) throws SQLException {
        stmt.setString(1, mobile_local + "_" + voucherNo + "_" + idWorker);
        for (Iterator iterator = jrr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            if ("mobile".equals(job.get("key"))) {
                stmt.setString(2, String.valueOf(job.get("value")));
            } else if ("carrier_name".equals(job.get("key"))) {
                stmt.setString(3, String.valueOf(job.get("value")));
            } else if ("carrier_idcard".equals(job.get("key"))) {
                stmt.setString(4, String.valueOf(job.get("value")));
            } else if ("reg_time".equals(job.get("key"))) {
                stmt.setString(5, String.valueOf(job.get("value")));
            } else if ("in_time".equals(job.get("key"))) {
                stmt.setString(6, String.valueOf(job.get("value")));
            } else if ("email".equals(job.get("key"))) {
                stmt.setString(7, String.valueOf(job.get("value")));
            } else if ("address".equals(job.get("key"))) {
                stmt.setString(8, String.valueOf(job.get("value")));
            } else if ("reliability".equals(job.get("key"))) {
                stmt.setString(9, String.valueOf(job.get("value")));
            } else if ("phone_attribution".equals(job.get("key"))) {
                stmt.setString(10, String.valueOf(job.get("value")));
            } else if ("live_address".equals(job.get("key"))) {
                stmt.setString(11, String.valueOf(job.get("value")));
            } else if ("available_balance".equals(job.get("key"))) {
                stmt.setString(12, String.valueOf(job.get("value")));
            } else if ("package_name".equals(job.get("key"))) {
                stmt.setString(13, String.valueOf(job.get("value")));
            } else if ("bill_certification_day".equals(job.get("key"))) {
                stmt.setString(14, String.valueOf(job.get("value")));
            }
            stmt.setString(15, String.valueOf(nowDate));
        }
    }

    //  行为监测表
    private static void behaviorCheck(String voucherNo, String mobile_local, PreparedStatement stmt, JSONArray jrr, String idWorker) throws SQLException {
        stmt.setString(1, mobile_local + "_" + voucherNo + "_" + idWorker);
        for (Iterator iterator = jrr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            if ("regular_circle".equals(job.get("check_point"))) { stmt.setString(2, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("phone_used_time".equals(job.get("check_point"))) {stmt.setString(3, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("phone_silent".equals(job.get("check_point"))) {stmt.setString(4, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("phone_power_off".equals(job.get("check_point"))) {stmt.setString(5, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_each_other".equals(job.get("check_point"))) {stmt.setString(6, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_macao".equals(job.get("check_point"))) {stmt.setString(7, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_110".equals(job.get("check_point"))) {stmt.setString(8, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_120".equals(job.get("check_point"))) {stmt.setString(9, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_lawyer".equals(job.get("check_point"))) {stmt.setString(10, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_court".equals(job.get("check_point"))) {stmt.setString(11, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_loan".equals(job.get("check_point"))) {stmt.setString(12, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_bank".equals(job.get("check_point"))) {stmt.setString(13, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_credit_card".equals(job.get("check_point"))) {stmt.setString(14, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_collection".equals(job.get("check_point"))) {stmt.setString(15, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("contact_night".equals(job.get("check_point"))) {stmt.setString(16, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("dwell_used_time".equals(job.get("check_point"))) {stmt.setString(17, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("ebusiness_info".equals(job.get("check_point"))) {stmt.setString(18, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("person_ebusiness_info".equals(job.get("check_point"))) {stmt.setString(19, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("virtual_buying".equals(job.get("check_point"))) {stmt.setString(20, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("lottery_buying".equals(job.get("check_point"))) {stmt.setString(21, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("person_addr_changed".equals(job.get("check_point"))) {stmt.setString(22, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("school_status".equals(job.get("check_point"))) {stmt.setString(23, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("education_info".equals(job.get("check_point"))) {stmt.setString(24, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("work_addr_info".equals(job.get("check_point"))) {stmt.setString(25, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("live_addr_info".equals(job.get("check_point"))) {stmt.setString(26, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("school_addr_info".equals(job.get("check_point"))) {stmt.setString(27, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            else if ("phone_call".equals(job.get("check_point"))) {stmt.setString(28, String.valueOf(job.get("result"))+"|"+String.valueOf(job.get("evidence")));}
            stmt.setString(29, String.valueOf(nowDate));
        }
    }

    //  朋友圈联系人数量表
    private static void summary(String voucherNo, String mobile_local, PreparedStatement stmt, JSONArray jrr, String idWorker) throws SQLException {
        stmt.setString(1, mobile_local + "_" + voucherNo + "_" + idWorker);
        for (Iterator iterator = jrr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            if ("friend_num_3m".equals(job.get("key"))) { stmt.setString(2, String.valueOf(job.get("value")));}
            else if ("good_friend_num_3m".equals(job.get("key"))) {stmt.setString(3, String.valueOf(job.get("value")));}
            else if ("friend_city_center_3m".equals(job.get("key"))) {stmt.setString(4, String.valueOf(job.get("value")));}
            else if ("is_city_match_friend_city_center_3m".equals(job.get("key"))) {stmt.setString(5, String.valueOf(job.get("value")));}
            else if ("inter_peer_num_3m".equals(job.get("key"))) {stmt.setString(6, String.valueOf(job.get("value")));}
            else if ("friend_num_6m".equals(job.get("key"))) {stmt.setString(7, String.valueOf(job.get("value")));}
            else if ("good_friend_num_6m".equals(job.get("key"))) {stmt.setString(8, String.valueOf(job.get("value")));}
            else if ("friend_city_center_6m".equals(job.get("key"))) {stmt.setString(9, String.valueOf(job.get("value")));}
            else if ("is_city_match_friend_city_center_6m".equals(job.get("key"))) {stmt.setString(10, String.valueOf(job.get("value")));}
            else if ("inter_peer_num_6m".equals(job.get("key"))) {stmt.setString(11, String.valueOf(job.get("value")));}
            stmt.setString(12, String.valueOf(nowDate));
        }
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        int files = 0;
        Connection conn = GetConnection.getConn_Cdsp_Riskcore();
        //  摩羯通讯详单  增量  and GMT_CREATED >= '2017-09-06 00:00:00'
        String sql = "select * from cdsp_hulu_access where QUERY_TYPE='MOXIE_CARRIER_REPORT' and  file_path IS NOT NULL "
                + "and GMT_CREATED >= '" + args[0] + "' " + "and GMT_CREATED < '" + args[1] + "' ";
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String voucher = rs.getString("VOUCHER_NO");  //  葫芦、摩羯
                String filePath = rs.getString("FILE_PATH");
                readFileByLines(voucher, filePath);
                System.out.println("=================Call=files=" + ++files);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString()+" 耗时为： " + (endtime - starttime));
    }
}