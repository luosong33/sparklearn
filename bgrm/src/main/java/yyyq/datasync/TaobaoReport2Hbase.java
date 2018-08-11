package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;

public class TaobaoReport2Hbase {
    public static void readFileByLines(String voucher_no, String filePath, String client_no, String client_name, String cert_no) {
        File file = null;
        try {
            file = new File(filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (file != null) {
            BufferedReader reader = null;
            Connection con = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
                String tempString = null;
                PreparedStatement stmt = null;
                con = GetConnection.getPhoenixConn();
                while ((tempString = reader.readLine()) != null) {
                    try {
                        JSONObject jobj = JSON.parseObject(tempString);
                        //  淘宝用户基本信息
                        JSONObject basic_info = (JSONObject) jobj.get("basic_info");
                        JSONObject user_and_account_basic_info = (JSONObject) basic_info.get("user_and_account_basic_info");
                        String alipay_account = user_and_account_basic_info.getString("alipay_account");
                        String taobao_email = user_and_account_basic_info.getString("taobao_email");
                        String taobao_name = user_and_account_basic_info.getString("taobao_name");
                        String taobao_phone_number = user_and_account_basic_info.getString("taobao_phone_number");
                        String taobao_vip_count = user_and_account_basic_info.getString("taobao_vip_count");
                        String taobao_vip_level = user_and_account_basic_info.getString("taobao_vip_level");
                        String tmall_apass = user_and_account_basic_info.getString("tmall_apass");
                        String tmall_level = user_and_account_basic_info.getString("tmall_level");
                        String tmall_vip_count = user_and_account_basic_info.getString("tmall_vip_count");

                        JSONObject wealth_info = (JSONObject) jobj.get("wealth_info");
                        JSONObject totalssets = (JSONObject) wealth_info.get("totalssets");
                        String balance = totalssets.getString("balance");
                        String huai_bei_can_use_limit = totalssets.getString("huai_bei_can_use_limit");
                        String huai_bei_limit = totalssets.getString("huai_bei_limit");
                        String total_profit = totalssets.getString("total_profit");
                        String yue_e_bao_amt = totalssets.getString("yue_e_bao_amt");

                        try {
                            con.setAutoCommit(false);
                            stmt = con.prepareStatement("upsert  into \"taobao_report_info\" (\"ID\" ,\"taobao_name\",\"taobao_email\",\"taobao_phone_number\",\"alipay_account\",\"taobao_vip_level\",\"taobao_vip_count\",\"tmall_level\",\"tmall_vip_count\",\"tmall_apass\",\"balance\",\"yue_e_bao_amt\",\"total_profit\",\"huai_bei_limit\",\"huai_bei_can_use_limit\",\"self_address_change\",\"self_city_change\",\"nonself_address_change\",\"self_address_cnt\",\"avg_self_address_cnt\",\"self_city_cnt\",\"avg_self_city_cnt\",\"nonself_address_cnt\",\"avg_nonself_address_cnt\",\"deliver_address_1\",\"deliver_address_2\",\"deliver_address_3\",\"deliver_city_1\",\"deliver_city_2\",\"deliver_city_3\",\"deliver_address_type_1\",\"deliver_address_type_2\",\"deliver_address_type_3\",\"use_month_1\",\"use_month_2\",\"use_month_3\",\"last_deliver_past_cur_1\",\"last_deliver_past_cur_2\",\"last_deliver_past_cur_3\",\"first_deliver_time_1\",\"first_deliver_time_2\",\"first_deliver_time_3\",\"last_deliver_time_1\",\"last_deliver_time_2\",\"last_deliver_time_3\",\"deliver_name_1\",\"deliver_name_2\",\"deliver_name_3\",\"deliver_phone_1\",\"deliver_phone_2\",\"deliver_phone_3\",\"deliver_amt_1\",\"deliver_amt_2\",\"deliver_amt_3\",\"deliver_cnt_1\",\"deliver_cnt_2\",\"deliver_cnt_3\",\"receiving_amt_1\",\"receiving_amt_2\",\"receiving_amt_3\",\"receiving_cnt_1\",\"receiving_cnt_2\",\"receiving_cnt_3\",\"max_deliver_name_3\",\"max_deliver_name_6\",\"max_deliver_phone_3\",\"max_deliver_phone_6\",\"max_deliver_address_3\",\"max_deliver_address_6\",\"max_deliver_city_3\",\"max_deliver_city_6\",\"is_default_3\",\"is_default_6\",\"max_cnt_3\",\"max_cnt_6\",\"default_deliver_cnt_3\",\"default_deliver_cnt_6\",\"max_deliver_city_cnt_3\",\"max_deliver_city_cnt_6\",\"total_category_cnt_1\",\"total_category_cnt_2\",\"total_category_cnt_3\",\"total_category_cnt_4\",\"total_category_cnt_5\",\"total_category_cnt_6\",\"total_category_cnt_sum\",\"total_consum_amt_1\",\"total_consum_amt_2\",\"total_consum_amt_3\",\"total_consum_amt_4\",\"total_consum_amt_5\",\"total_consum_amt_6\",\"total_consum_amt_sum\",\"total_consum_times_1\",\"total_consum_times_2\",\"total_consum_times_3\",\"total_consum_times_4\",\"total_consum_times_5\",\"total_consum_times_6\",\"total_consum_times_sum\",\"lottery_amt_1\",\"lottery_amt_2\",\"lottery_amt_3\",\"lottery_amt_4\",\"lottery_amt_5\",\"lottery_amt_6\",\"lottery_amt_sum\",\"lottery_cnt_1\",\"lottery_cnt_2\",\"lottery_cnt_3\",\"lottery_cnt_4\",\"lottery_cnt_5\",\"lottery_cnt_6\",\"lottery_cnt_sum\",\"lottery_rate_1\",\"lottery_rate_2\",\"lottery_rate_3\",\"lottery_rate_4\",\"lottery_rate_5\",\"lottery_rate_6\",\"lottery_rate_sum\",\"virtual_goods_amt_1\",\"virtual_goods_amt_2\",\"virtual_goods_amt_3\",\"virtual_goods_amt_4\",\"virtual_goods_amt_5\",\"virtual_goods_amt_6\",\"virtual_goods_amt_sum\",\"virtual_goods_cnt_1\",\"virtual_goods_cnt_2\",\"virtual_goods_cnt_3\",\"virtual_goods_cnt_4\",\"virtual_goods_cnt_5\",\"virtual_goods_cnt_6\",\"virtual_goods_cnt_sum\",\"virtual_goods_rate_1\",\"virtual_goods_rate_2\",\"virtual_goods_rate_3\",\"virtual_goods_rate_4\",\"virtual_goods_rate_5\",\"virtual_goods_rate_6\",\"virtual_goods_rate_sum\",\"self_category_cnt_1\",\"self_category_cnt_2\",\"self_category_cnt_3\",\"self_category_cnt_4\",\"self_category_cnt_5\",\"self_category_cnt_6\",\"self_category_cnt_sum\",\"self_consum_amt_1\",\"self_consum_amt_2\",\"self_consum_amt_3\",\"self_consum_amt_4\",\"self_consum_amt_5\",\"self_consum_amt_6\",\"self_consum_amt_sum\",\"self_consum_times_1\",\"self_consum_times_2\",\"self_consum_times_3\",\"self_consum_times_4\",\"self_consum_times_5\",\"self_consum_times_6\",\"self_consum_times_sum\",\"loadtime\",\"client_name\",\"cert_no\")values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                            stmt.setString(1, client_no + "_" + voucher_no);
                            stmt.setString(2, taobao_name != null ? taobao_name : "");
                            stmt.setString(3, taobao_email != null ? taobao_email : "");
                            stmt.setString(4, taobao_phone_number != null ? taobao_phone_number : "");
                            stmt.setString(5, alipay_account != null ? alipay_account : "");
                            stmt.setString(6, taobao_vip_level != null ? taobao_vip_level : "");
                            stmt.setString(7, taobao_vip_count != null ? taobao_vip_count : "");
                            stmt.setString(8, tmall_level != null ? tmall_level : "");
                            stmt.setString(9, tmall_vip_count != null ? tmall_vip_count : "");
                            stmt.setString(10, tmall_apass != null ? tmall_apass : "");
                            stmt.setString(11, balance != null ? balance : "");
                            stmt.setString(12, yue_e_bao_amt != null ? yue_e_bao_amt : "");
                            stmt.setString(13, total_profit != null ? total_profit : "");
                            stmt.setString(14, huai_bei_limit != null ? huai_bei_limit : "");
                            stmt.setString(15, huai_bei_can_use_limit != null ? huai_bei_can_use_limit : "");

                            stmt.setString(164, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                            stmt.setString(165, client_name);
                            stmt.setString(166, cert_no);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                        JSONObject consumption_analysis = (JSONObject) jobj.get("consumption_analysis");
                        //  类目一
                        category_one(stmt, consumption_analysis);
                        // 类目二
                        category_two(stmt, consumption_analysis);
                        // 类目三
                        category_three(stmt, consumption_analysis);
                        // 类目四
                        JSONObject address_analysis = (JSONObject) jobj.get("address_analysis");
                        category_four(stmt, address_analysis);

                        try {
                            stmt.addBatch();
                            stmt.executeBatch();
                            con.commit();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                try {
                    con.close();
                    reader.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (con != null) { con.close(); }
                    if (reader != null) { reader.close(); }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void category_four(PreparedStatement stmt, JSONObject address_analysis) {
        JSONObject fundamental_point_analysis = (JSONObject) address_analysis.get("fundamental_point_analysis");
        String self_address_change = String.valueOf(fundamental_point_analysis.get("self_address_change"));
        String self_city_change = String.valueOf(fundamental_point_analysis.get("self_city_change"));
        String nonself_address_change = String.valueOf(fundamental_point_analysis.get("nonself_address_change"));
        String self_address_cnt = String.valueOf(fundamental_point_analysis.get("self_address_cnt"));
        String avg_self_address_cnt = String.valueOf(fundamental_point_analysis.get("avg_self_address_cnt"));
        String self_city_cnt = String.valueOf(fundamental_point_analysis.get("self_city_cnt"));
        String avg_self_city_cnt = String.valueOf(fundamental_point_analysis.get("avg_self_city_cnt"));
        String nonself_address_cnt = String.valueOf(fundamental_point_analysis.get("nonself_address_cnt"));
        String avg_nonself_address_cnt = String.valueOf(fundamental_point_analysis.get("avg_nonself_address_cnt"));

        //  2
        JSONObject commonly_used_address = (JSONObject) address_analysis.get("commonly_used_address");
        JSONObject deliver_address = (JSONObject) commonly_used_address.get("deliver_address");
        Set<String> deliver_address_keys = deliver_address.keySet();
        Object[] deliver_address_ksysArr = deliver_address_keys.toArray();
        Arrays.sort(deliver_address_ksysArr);
        String deliver_address_1 = "";
        String deliver_address_2 = "";
        String deliver_address_3 = "";
        for (int i = 0; i < deliver_address_ksysArr.length; i++) {
            if (i == 0) deliver_address_1 = deliver_address.getString(String.valueOf(deliver_address_ksysArr[0]));
            if (i == 1) deliver_address_2 = deliver_address.getString(String.valueOf(deliver_address_ksysArr[1]));
            if (i == 2) deliver_address_3 = deliver_address.getString(String.valueOf(deliver_address_ksysArr[2]));
        }

        JSONObject deliver_city = (JSONObject) commonly_used_address.get("deliver_city");
        Set<String> deliver_city_keys = deliver_city.keySet();
        Object[] deliver_city_ksysArr = deliver_city_keys.toArray();
        Arrays.sort(deliver_city_ksysArr);
        String deliver_city_1 = "";
        String deliver_city_2 = "";
        String deliver_city_3 = "";
        for (int i = 0; i < deliver_address_ksysArr.length; i++) {
            if (i == 0) deliver_city_1 = deliver_city.getString(String.valueOf(deliver_city_ksysArr[0]));
            if (i == 1) deliver_city_2 = deliver_city.getString(String.valueOf(deliver_city_ksysArr[1]));
            if (i == 2) deliver_city_3 = deliver_city.getString(String.valueOf(deliver_city_ksysArr[2]));
        }

        JSONObject deliver_address_type = (JSONObject) commonly_used_address.get("deliver_address_type");
        Set<String> deliver_address_type_keys = deliver_address_type.keySet();
        Object[] deliver_address_type_ksysArr = deliver_address_type_keys.toArray();
        Arrays.sort(deliver_address_type_ksysArr);
        String deliver_address_type_1 = "";
        String deliver_address_type_2 = "";
        String deliver_address_type_3 = "";
        for (int i = 0; i < deliver_address_type_ksysArr.length; i++) {
            if (i == 0)
                deliver_address_type_1 = deliver_address_type.getString(String.valueOf(deliver_address_type_ksysArr[0]));
            if (i == 1)
                deliver_address_type_2 = deliver_address_type.getString(String.valueOf(deliver_address_type_ksysArr[1]));
            if (i == 2)
                deliver_address_type_3 = deliver_address_type.getString(String.valueOf(deliver_address_type_ksysArr[2]));
        }

        JSONObject use_month = (JSONObject) commonly_used_address.get("use_month");
        Set<String> use_month_keys = use_month.keySet();
        Object[] use_month_ksysArr = use_month_keys.toArray();
        Arrays.sort(use_month_ksysArr);
        String use_month_1 = "";
        String use_month_2 = "";
        String use_month_3 = "";
        for (int i = 0; i < use_month_ksysArr.length; i++) {
            if (i == 0) use_month_1 = use_month.getString(String.valueOf(use_month_ksysArr[0]));
            if (i == 1) use_month_2 = use_month.getString(String.valueOf(use_month_ksysArr[1]));
            if (i == 2) use_month_3 = use_month.getString(String.valueOf(use_month_ksysArr[2]));
        }

        JSONObject last_deliver_past_cur = (JSONObject) commonly_used_address.get("last_deliver_past_cur");
        Set<String> last_deliver_past_cur_keys = last_deliver_past_cur.keySet();
        Object[] last_deliver_past_cur_ksysArr = last_deliver_past_cur_keys.toArray();
        Arrays.sort(last_deliver_past_cur_ksysArr);
        String last_deliver_past_cur_1 = "";
        String last_deliver_past_cur_2 = "";
        String last_deliver_past_cur_3 = "";
        for (int i = 0; i < last_deliver_past_cur_ksysArr.length; i++) {
            if (i == 0)
                last_deliver_past_cur_1 = last_deliver_past_cur.getString(String.valueOf(last_deliver_past_cur_ksysArr[0]));
            if (i == 1)
                last_deliver_past_cur_2 = last_deliver_past_cur.getString(String.valueOf(last_deliver_past_cur_ksysArr[1]));
            if (i == 2)
                last_deliver_past_cur_3 = last_deliver_past_cur.getString(String.valueOf(last_deliver_past_cur_ksysArr[2]));
        }

        JSONObject first_deliver_time = (JSONObject) commonly_used_address.get("first_deliver_time");
        Set<String> first_deliver_time_keys = first_deliver_time.keySet();
        Object[] first_deliver_time_ksysArr = first_deliver_time_keys.toArray();
        Arrays.sort(first_deliver_time_ksysArr);
        String first_deliver_time_1 = "";
        String first_deliver_time_2 = "";
        String first_deliver_time_3 = "";
        for (int i = 0; i < first_deliver_time_ksysArr.length; i++) {
            if (i == 0)
                first_deliver_time_1 = first_deliver_time.getString(String.valueOf(first_deliver_time_ksysArr[0]));
            if (i == 1)
                first_deliver_time_2 = first_deliver_time.getString(String.valueOf(first_deliver_time_ksysArr[1]));
            if (i == 2)
                first_deliver_time_3 = first_deliver_time.getString(String.valueOf(first_deliver_time_ksysArr[2]));
        }

        JSONObject last_deliver_time = (JSONObject) commonly_used_address.get("last_deliver_time");
        Set<String> last_deliver_time_keys = last_deliver_time.keySet();
        Object[] last_deliver_time_ksysArr = last_deliver_time_keys.toArray();
        Arrays.sort(last_deliver_time_ksysArr);
        String last_deliver_time_1 = "";
        String last_deliver_time_2 = "";
        String last_deliver_time_3 = "";
        for (int i = 0; i < last_deliver_time_ksysArr.length; i++) {
            if (i == 0) last_deliver_time_1 = last_deliver_time.getString(String.valueOf(last_deliver_time_ksysArr[0]));
            if (i == 1) last_deliver_time_2 = last_deliver_time.getString(String.valueOf(last_deliver_time_ksysArr[1]));
            if (i == 2) last_deliver_time_3 = last_deliver_time.getString(String.valueOf(last_deliver_time_ksysArr[2]));
        }

        JSONObject deliver_name = (JSONObject) commonly_used_address.get("deliver_name");
        Set<String> deliver_name_keys = deliver_name.keySet();
        Object[] deliver_name_ksysArr = deliver_name_keys.toArray();
        Arrays.sort(deliver_name_ksysArr);
        String deliver_name_1 = "";
        String deliver_name_2 = "";
        String deliver_name_3 = "";
        for (int i = 0; i < deliver_name_ksysArr.length; i++) {
            if (i == 0) deliver_name_1 = deliver_name.getString(String.valueOf(deliver_name_ksysArr[0]));
            if (i == 1) deliver_name_2 = deliver_name.getString(String.valueOf(deliver_name_ksysArr[1]));
            if (i == 2) deliver_name_3 = deliver_name.getString(String.valueOf(deliver_name_ksysArr[2]));
        }

        JSONObject deliver_phone = (JSONObject) commonly_used_address.get("deliver_phone");
        Set<String> deliver_phone_keys = deliver_phone.keySet();
        Object[] deliver_phone_ksysArr = deliver_phone_keys.toArray();
        Arrays.sort(deliver_phone_ksysArr);
        String deliver_phone_1 = "";
        String deliver_phone_2 = "";
        String deliver_phone_3 = "";
        for (int i = 0; i < deliver_phone_ksysArr.length; i++) {
            if (i == 0) deliver_phone_1 = deliver_phone.getString(String.valueOf(deliver_phone_ksysArr[0]));
            if (i == 1) deliver_phone_2 = deliver_phone.getString(String.valueOf(deliver_phone_ksysArr[1]));
            if (i == 2) deliver_phone_3 = deliver_phone.getString(String.valueOf(deliver_phone_ksysArr[2]));
        }

        JSONObject deliver_amt = (JSONObject) commonly_used_address.get("deliver_amt");
        Set<String> deliver_amt_keys = deliver_amt.keySet();
        Object[] deliver_amt_ksysArr = deliver_amt_keys.toArray();
        Arrays.sort(deliver_amt_ksysArr);
        String deliver_amt_1 = "";
        String deliver_amt_2 = "";
        String deliver_amt_3 = "";
        for (int i = 0; i < deliver_amt_ksysArr.length; i++) {
            if (i == 0) deliver_amt_1 = deliver_amt.getString(String.valueOf(deliver_amt_ksysArr[0]));
            if (i == 1) deliver_amt_2 = deliver_amt.getString(String.valueOf(deliver_amt_ksysArr[1]));
            if (i == 2) deliver_amt_3 = deliver_amt.getString(String.valueOf(deliver_amt_ksysArr[2]));
        }

        JSONObject deliver_cnt = (JSONObject) commonly_used_address.get("deliver_cnt");
        Set<String> deliver_cnt_keys = deliver_cnt.keySet();
        Object[] deliver_cnt_ksysArr = deliver_cnt_keys.toArray();
        Arrays.sort(deliver_cnt_ksysArr);
        String deliver_cnt_1 = "";
        String deliver_cnt_2 = "";
        String deliver_cnt_3 = "";
        for (int i = 0; i < deliver_cnt_ksysArr.length; i++) {
            if (i == 0) deliver_cnt_1 = deliver_cnt.getString(String.valueOf(deliver_cnt_ksysArr[0]));
            if (i == 1) deliver_cnt_2 = deliver_cnt.getString(String.valueOf(deliver_cnt_ksysArr[1]));
            if (i == 2) deliver_cnt_3 = deliver_cnt.getString(String.valueOf(deliver_cnt_ksysArr[2]));
        }

        JSONObject receiving_amt = (JSONObject) commonly_used_address.get("receiving_amt");
        Set<String> receiving_amt_keys = receiving_amt.keySet();
        Object[] receiving_amt_ksysArr = receiving_amt_keys.toArray();
        Arrays.sort(receiving_amt_ksysArr);
        String receiving_amt_1 = "";
        String receiving_amt_2 = "";
        String receiving_amt_3 = "";
        for (int i = 0; i < receiving_amt_ksysArr.length; i++) {
            if (i == 0) receiving_amt_1 = receiving_amt.getString(String.valueOf(receiving_amt_ksysArr[0]));
            if (i == 1) receiving_amt_2 = receiving_amt.getString(String.valueOf(receiving_amt_ksysArr[1]));
            if (i == 2) receiving_amt_3 = receiving_amt.getString(String.valueOf(receiving_amt_ksysArr[2]));
        }

        JSONObject receiving_cnt = (JSONObject) commonly_used_address.get("receiving_cnt");
        Set<String> receiving_cnt_keys = receiving_cnt.keySet();
        Object[] receiving_cnt_ksysArr = receiving_cnt_keys.toArray();
        Arrays.sort(receiving_cnt_ksysArr);
        String receiving_cnt_1 = "";
        String receiving_cnt_2 = "";
        String receiving_cnt_3 = "";
        for (int i = 0; i < receiving_cnt_ksysArr.length; i++) {
            if (i == 0) receiving_cnt_1 = receiving_cnt.getString(String.valueOf(receiving_cnt_ksysArr[0]));
            if (i == 1) receiving_cnt_2 = receiving_cnt.getString(String.valueOf(receiving_cnt_ksysArr[1]));
            if (i == 2) receiving_cnt_3 = receiving_cnt.getString(String.valueOf(receiving_cnt_ksysArr[2]));
        }

        //  3
        JSONObject receipt_details = (JSONObject) address_analysis.get("receipt_details");

        String max_deliver_name_3 = String.valueOf(receipt_details.getString("max_deliver_name_3"));
        String max_deliver_name_6 = String.valueOf(receipt_details.getString("max_deliver_name_6"));
        String max_deliver_phone_3 = String.valueOf(receipt_details.getString("max_deliver_phone_3"));
        String max_deliver_phone_6 = String.valueOf(receipt_details.getString("max_deliver_phone_6"));
        String max_deliver_address_3 = String.valueOf(receipt_details.getString("max_deliver_address_3"));
        String max_deliver_address_6 = String.valueOf(receipt_details.getString("max_deliver_address_6"));
        String max_deliver_city_3 = String.valueOf(receipt_details.getString("max_deliver_city_3"));
        String max_deliver_city_6 = String.valueOf(receipt_details.getString("max_deliver_city_6"));
        String is_default_3 = String.valueOf(receipt_details.getString("is_default_3"));
        String is_default_6 = String.valueOf(receipt_details.getString("is_default_6"));
        String max_cnt_3 = String.valueOf(receipt_details.getString("max_cnt_3"));
        String max_cnt_6 = String.valueOf(receipt_details.getString("max_cnt_6"));
        String default_deliver_cnt_3 = String.valueOf(receipt_details.getString("default_deliver_cnt_3"));
        String default_deliver_cnt_6 = String.valueOf(receipt_details.getString("default_deliver_cnt_6"));
        String max_deliver_city_cnt_3 = String.valueOf(receipt_details.getString("max_deliver_city_cnt_3"));
        String max_deliver_city_cnt_6 = String.valueOf(receipt_details.getString("max_deliver_city_cnt_6"));

        try {
            stmt.setString(16, self_address_change);
            stmt.setString(17, self_city_change);
            stmt.setString(18, nonself_address_change);
            stmt.setString(19, self_address_cnt);
            stmt.setString(20, avg_self_address_cnt);
            stmt.setString(21, self_city_cnt);
            stmt.setString(22, avg_self_city_cnt);
            stmt.setString(23, nonself_address_cnt);
            stmt.setString(24, avg_nonself_address_cnt);
            stmt.setString(25, deliver_address_1);
            stmt.setString(26, deliver_address_2);
            stmt.setString(27, deliver_address_3);
            stmt.setString(28, deliver_city_1);
            stmt.setString(29, deliver_city_2);
            stmt.setString(30, deliver_city_3);
            stmt.setString(31, deliver_address_type_1);
            stmt.setString(32, deliver_address_type_2);
            stmt.setString(33, deliver_address_type_3);
            stmt.setString(34, use_month_1);
            stmt.setString(35, use_month_2);
            stmt.setString(36, use_month_3);
            stmt.setString(37, last_deliver_past_cur_1);
            stmt.setString(38, last_deliver_past_cur_2);
            stmt.setString(39, last_deliver_past_cur_3);
            stmt.setString(40, first_deliver_time_1);
            stmt.setString(41, first_deliver_time_2);
            stmt.setString(42, first_deliver_time_3);
            stmt.setString(43, last_deliver_time_1);
            stmt.setString(44, last_deliver_time_2);
            stmt.setString(45, last_deliver_time_3);
            stmt.setString(46, deliver_name_1);
            stmt.setString(47, deliver_name_2);
            stmt.setString(48, deliver_name_3);
            stmt.setString(49, deliver_phone_1);
            stmt.setString(50, deliver_phone_2);
            stmt.setString(51, deliver_phone_3);
            stmt.setString(52, deliver_amt_1);
            stmt.setString(53, deliver_amt_2);
            stmt.setString(54, deliver_amt_3);
            stmt.setString(55, deliver_cnt_1);
            stmt.setString(56, deliver_cnt_2);
            stmt.setString(57, deliver_cnt_3);
            stmt.setString(58, receiving_amt_1);
            stmt.setString(59, receiving_amt_2);
            stmt.setString(60, receiving_amt_3);
            stmt.setString(61, receiving_cnt_1);
            stmt.setString(62, receiving_cnt_2);
            stmt.setString(63, receiving_cnt_3);
            stmt.setString(64, max_deliver_name_3);
            stmt.setString(65, max_deliver_name_6);
            stmt.setString(66, max_deliver_phone_3);
            stmt.setString(67, max_deliver_phone_6);
            stmt.setString(68, max_deliver_address_3);
            stmt.setString(69, max_deliver_address_6);
            stmt.setString(70, max_deliver_city_3);
            stmt.setString(71, max_deliver_city_6);
            stmt.setString(72, is_default_3);
            stmt.setString(73, is_default_6);
            stmt.setString(74, max_cnt_3);
            stmt.setString(75, max_cnt_6);
            stmt.setString(76, default_deliver_cnt_3);
            stmt.setString(77, default_deliver_cnt_6);
            stmt.setString(78, max_deliver_city_cnt_3);
            stmt.setString(79, max_deliver_city_cnt_6);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void category_three(PreparedStatement stmt, JSONObject consumption_analysis) {
        JSONObject receiving_consumption = (JSONObject) consumption_analysis.get("receiving_consumption");
        JSONObject self_category_cnt = (JSONObject) receiving_consumption.get("self_category_cnt");
        Set<String> keys = self_category_cnt.keySet();
        Object[] ksysArr = keys.toArray();
        Arrays.sort(ksysArr);
        String self_category_cnt_1 = "";
        String self_category_cnt_2 = "";
        String self_category_cnt_3 = "";
        String self_category_cnt_4 = "";
        String self_category_cnt_5 = "";
        String self_category_cnt_6 = "";
        for (int i = 0; i < ksysArr.length; i++) {
            if (i == 0)
                self_category_cnt_1 = ksysArr[0] + ":" + self_category_cnt.getString(String.valueOf(ksysArr[0]));
            if (i == 1)
                self_category_cnt_2 = ksysArr[1] + ":" + self_category_cnt.getString(String.valueOf(ksysArr[1]));
            if (i == 2)
                self_category_cnt_3 = ksysArr[2] + ":" + self_category_cnt.getString(String.valueOf(ksysArr[2]));
            if (i == 3)
                self_category_cnt_4 = ksysArr[3] + ":" + self_category_cnt.getString(String.valueOf(ksysArr[3]));
            if (i == 4)
                self_category_cnt_5 = ksysArr[4] + ":" + self_category_cnt.getString(String.valueOf(ksysArr[4]));
            if (i == 5)
                self_category_cnt_6 = ksysArr[5] + ":" + self_category_cnt.getString(String.valueOf(ksysArr[5]));
        }
        String self_category_cnt_sum = self_category_cnt.getString("sum");

        JSONObject self_consum_amt = (JSONObject) receiving_consumption.get("self_consum_amt");
        Set<String> self_consum_amt_keys = self_consum_amt.keySet();
        Object[] self_consum_amt_keysArr = self_consum_amt_keys.toArray();
        Arrays.sort(self_consum_amt_keysArr);
        String self_consum_amt_1 = "";
        String self_consum_amt_2 = "";
        String self_consum_amt_3 = "";
        String self_consum_amt_4 = "";
        String self_consum_amt_5 = "";
        String self_consum_amt_6 = "";
        for (int i = 0; i < self_consum_amt_keysArr.length; i++) {
            if (i == 0)
                self_consum_amt_1 = self_consum_amt_keysArr[0] + ":" + self_consum_amt.getString(String.valueOf(self_consum_amt_keysArr[0]));
            if (i == 1)
                self_consum_amt_2 = self_consum_amt_keysArr[1] + ":" + self_consum_amt.getString(String.valueOf(self_consum_amt_keysArr[1]));
            if (i == 2)
                self_consum_amt_3 = self_consum_amt_keysArr[2] + ":" + self_consum_amt.getString(String.valueOf(self_consum_amt_keysArr[2]));
            if (i == 3)
                self_consum_amt_4 = self_consum_amt_keysArr[3] + ":" + self_consum_amt.getString(String.valueOf(self_consum_amt_keysArr[3]));
            if (i == 4)
                self_consum_amt_5 = self_consum_amt_keysArr[4] + ":" + self_consum_amt.getString(String.valueOf(self_consum_amt_keysArr[4]));
            if (i == 5)
                self_consum_amt_6 = self_consum_amt_keysArr[5] + ":" + self_consum_amt.getString(String.valueOf(self_consum_amt_keysArr[5]));
        }
        String self_consum_amt_sum = self_consum_amt.getString("sum");

        JSONObject self_consum_times = (JSONObject) receiving_consumption.get("self_consum_times");
        Set<String> receiving_times_sum_keys = self_consum_amt.keySet();
        Object[] receiving_times_sum_keysArr = receiving_times_sum_keys.toArray();
        Arrays.sort(receiving_times_sum_keysArr);
        String self_consum_times_1 = "";
        String self_consum_times_2 = "";
        String self_consum_times_3 = "";
        String self_consum_times_4 = "";
        String self_consum_times_5 = "";
        String self_consum_times_6 = "";
        for (int i = 0; i < receiving_times_sum_keysArr.length; i++) {
            if (i == 0)
                self_consum_times_1 = receiving_times_sum_keysArr[0] + ":" + self_consum_times.getString(String.valueOf(receiving_times_sum_keysArr[0]));
            if (i == 1)
                self_consum_times_2 = receiving_times_sum_keysArr[1] + ":" + self_consum_times.getString(String.valueOf(receiving_times_sum_keysArr[1]));
            if (i == 2)
                self_consum_times_3 = receiving_times_sum_keysArr[2] + ":" + self_consum_times.getString(String.valueOf(receiving_times_sum_keysArr[2]));
            if (i == 3)
                self_consum_times_4 = receiving_times_sum_keysArr[3] + ":" + self_consum_times.getString(String.valueOf(receiving_times_sum_keysArr[3]));
            if (i == 4)
                self_consum_times_5 = receiving_times_sum_keysArr[4] + ":" + self_consum_times.getString(String.valueOf(receiving_times_sum_keysArr[4]));
            if (i == 5)
                self_consum_times_6 = receiving_times_sum_keysArr[5] + ":" + self_consum_times.getString(String.valueOf(receiving_times_sum_keysArr[5]));
        }
        String self_consum_times_sum = self_consum_times.getString("sum");

        try {
            stmt.setString(143, self_category_cnt_1);
            stmt.setString(144, self_category_cnt_2);
            stmt.setString(145, self_category_cnt_3);
            stmt.setString(146, self_category_cnt_4);
            stmt.setString(147, self_category_cnt_5);
            stmt.setString(148, self_category_cnt_6);
            stmt.setString(149, self_category_cnt_sum);
            stmt.setString(150, self_consum_amt_1);
            stmt.setString(151, self_consum_amt_2);
            stmt.setString(152, self_consum_amt_3);
            stmt.setString(153, self_consum_amt_4);
            stmt.setString(154, self_consum_amt_5);
            stmt.setString(155, self_consum_amt_6);
            stmt.setString(156, self_consum_amt_sum);
            stmt.setString(157, self_consum_times_1);
            stmt.setString(158, self_consum_times_2);
            stmt.setString(159, self_consum_times_3);
            stmt.setString(160, self_consum_times_4);
            stmt.setString(161, self_consum_times_5);
            stmt.setString(162, self_consum_times_6);
            stmt.setString(163, self_consum_times_sum);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void category_two(PreparedStatement stmt, JSONObject consumption_analysis) {
        JSONObject special_consumption = (JSONObject) consumption_analysis.get("special_consumption");
        JSONObject lottery_amt = (JSONObject) special_consumption.get("lottery_amt");
        Set<String> lottery_amt_keys = lottery_amt.keySet();
        Object[] lottery_amt_keysArr = lottery_amt_keys.toArray();
        Arrays.sort(lottery_amt_keysArr);
        String lottery_amt_1 = "";
        String lottery_amt_2 = "";
        String lottery_amt_3 = "";
        String lottery_amt_4 = "";
        String lottery_amt_5 = "";
        String lottery_amt_6 = "";
        for (int i = 0; i < lottery_amt_keysArr.length; i++) {
            if (i == 0)
                lottery_amt_1 = lottery_amt_keysArr[0] + ":" + lottery_amt.getString(String.valueOf(lottery_amt_keysArr[0]));
            if (i == 1)
                lottery_amt_2 = lottery_amt_keysArr[1] + ":" + lottery_amt.getString(String.valueOf(lottery_amt_keysArr[1]));
            if (i == 2)
                lottery_amt_3 = lottery_amt_keysArr[2] + ":" + lottery_amt.getString(String.valueOf(lottery_amt_keysArr[2]));
            if (i == 3)
                lottery_amt_4 = lottery_amt_keysArr[3] + ":" + lottery_amt.getString(String.valueOf(lottery_amt_keysArr[3]));
            if (i == 4)
                lottery_amt_5 = lottery_amt_keysArr[4] + ":" + lottery_amt.getString(String.valueOf(lottery_amt_keysArr[4]));
            if (i == 5)
                lottery_amt_6 = lottery_amt_keysArr[5] + ":" + lottery_amt.getString(String.valueOf(lottery_amt_keysArr[5]));
        }
        String lottery_amt_sum = lottery_amt.getString("sum");

        JSONObject lottery_cnt = (JSONObject) special_consumption.get("lottery_cnt");
        Set<String> lottery_cnt_keys = lottery_cnt.keySet();
        Object[] lottery_cnt_keysArr = lottery_cnt_keys.toArray();
        Arrays.sort(lottery_cnt_keysArr);
        String lottery_cnt_1 = "";
        String lottery_cnt_2 = "";
        String lottery_cnt_3 = "";
        String lottery_cnt_4 = "";
        String lottery_cnt_5 = "";
        String lottery_cnt_6 = "";
        for (int i = 0; i < lottery_cnt_keysArr.length; i++) {
            if (i == 0)
                lottery_cnt_1 = lottery_cnt_keysArr[0] + ":" + lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[0]));
            if (i == 1)
                lottery_cnt_2 = lottery_cnt_keysArr[1] + ":" + lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[1]));
            if (i == 2)
                lottery_cnt_3 = lottery_cnt_keysArr[2] + ":" + lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[2]));
            if (i == 3)
                lottery_cnt_4 = lottery_cnt_keysArr[3] + ":" + lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[3]));
            if (i == 4)
                lottery_cnt_5 = lottery_cnt_keysArr[4] + ":" + lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[4]));
            if (i == 5)
                lottery_cnt_6 = lottery_cnt_keysArr[5] + ":" + lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[5]));
        }
        String lottery_cnt_sum = lottery_cnt.getString("sum");

        JSONObject lottery_rate = (JSONObject) special_consumption.get("lottery_rate");
        Set<String> lottery_rate_keys = lottery_rate.keySet();
        Object[] lottery_rate_keysArr = lottery_rate_keys.toArray();
        Arrays.sort(lottery_rate_keysArr);
        String lottery_rate_1 = "";
        String lottery_rate_2 = "";
        String lottery_rate_3 = "";
        String lottery_rate_4 = "";
        String lottery_rate_5 = "";
        String lottery_rate_6 = "";
        for (int i = 0; i < lottery_rate_keysArr.length; i++) {
            if (i == 0)
                lottery_rate_1 = lottery_rate_keysArr[0] + ":" + lottery_rate.getString(String.valueOf(lottery_rate_keysArr[0]));
            if (i == 1)
                lottery_rate_2 = lottery_rate_keysArr[1] + ":" + lottery_rate.getString(String.valueOf(lottery_rate_keysArr[1]));
            if (i == 2)
                lottery_rate_3 = lottery_rate_keysArr[2] + ":" + lottery_rate.getString(String.valueOf(lottery_rate_keysArr[2]));
            if (i == 3)
                lottery_rate_4 = lottery_rate_keysArr[3] + ":" + lottery_rate.getString(String.valueOf(lottery_rate_keysArr[3]));
            if (i == 4)
                lottery_rate_5 = lottery_rate_keysArr[4] + ":" + lottery_rate.getString(String.valueOf(lottery_rate_keysArr[4]));
            if (i == 5)
                lottery_rate_6 = lottery_rate_keysArr[5] + ":" + lottery_rate.getString(String.valueOf(lottery_rate_keysArr[5]));
        }
        String lottery_rate_sum = lottery_rate.getString("sum");

        JSONObject virtual_goods_amt = (JSONObject) special_consumption.get("virtual_goods_amt");
        Set<String> virtual_goods_amt_keys = virtual_goods_amt.keySet();
        Object[] virtual_goods_amt_keysArr = virtual_goods_amt_keys.toArray();
        Arrays.sort(virtual_goods_amt_keysArr);
        String virtual_goods_amt_1 = "";
        String virtual_goods_amt_2 = "";
        String virtual_goods_amt_3 = "";
        String virtual_goods_amt_4 = "";
        String virtual_goods_amt_5 = "";
        String virtual_goods_amt_6 = "";
        for (int i = 0; i < virtual_goods_amt_keysArr.length; i++) {
            if (i == 0)
                virtual_goods_amt_1 = virtual_goods_amt_keysArr[0] + ":" + virtual_goods_amt.getString(String.valueOf(virtual_goods_amt_keysArr[0]));
            if (i == 1)
                virtual_goods_amt_2 = virtual_goods_amt_keysArr[1] + ":" + virtual_goods_amt.getString(String.valueOf(virtual_goods_amt_keysArr[1]));
            if (i == 2)
                virtual_goods_amt_3 = virtual_goods_amt_keysArr[2] + ":" + virtual_goods_amt.getString(String.valueOf(virtual_goods_amt_keysArr[2]));
            if (i == 3)
                virtual_goods_amt_4 = virtual_goods_amt_keysArr[3] + ":" + virtual_goods_amt.getString(String.valueOf(virtual_goods_amt_keysArr[3]));
            if (i == 4)
                virtual_goods_amt_5 = virtual_goods_amt_keysArr[4] + ":" + virtual_goods_amt.getString(String.valueOf(virtual_goods_amt_keysArr[4]));
            if (i == 5)
                virtual_goods_amt_6 = virtual_goods_amt_keysArr[5] + ":" + virtual_goods_amt.getString(String.valueOf(virtual_goods_amt_keysArr[5]));
        }
        String virtual_goods_amt_sum = virtual_goods_amt.getString("sum");

        JSONObject virtual_goods_cnt = (JSONObject) special_consumption.get("virtual_goods_cnt");
        Set<String> virtual_goods_cnt_keys = virtual_goods_cnt.keySet();
        Object[] virtual_goods_cnt_keysArr = virtual_goods_cnt_keys.toArray();
        Arrays.sort(virtual_goods_cnt_keysArr);
        String virtual_goods_cnt_1 = "";
        String virtual_goods_cnt_2 = "";
        String virtual_goods_cnt_3 = "";
        String virtual_goods_cnt_4 = "";
        String virtual_goods_cnt_5 = "";
        String virtual_goods_cnt_6 = "";
        for (int i = 0; i < virtual_goods_cnt_keysArr.length; i++) {
            if (i == 0)
                virtual_goods_cnt_1 = virtual_goods_cnt_keysArr[0] + ":" + virtual_goods_cnt.getString(String.valueOf(virtual_goods_cnt_keysArr[0]));
            if (i == 1)
                virtual_goods_cnt_2 = virtual_goods_cnt_keysArr[1] + ":" + virtual_goods_cnt.getString(String.valueOf(virtual_goods_cnt_keysArr[1]));
            if (i == 2)
                virtual_goods_cnt_3 = virtual_goods_cnt_keysArr[2] + ":" + virtual_goods_cnt.getString(String.valueOf(virtual_goods_cnt_keysArr[2]));
            if (i == 3)
                virtual_goods_cnt_4 = virtual_goods_cnt_keysArr[3] + ":" + virtual_goods_cnt.getString(String.valueOf(virtual_goods_cnt_keysArr[3]));
            if (i == 4)
                virtual_goods_cnt_5 = virtual_goods_cnt_keysArr[4] + ":" + virtual_goods_cnt.getString(String.valueOf(virtual_goods_cnt_keysArr[4]));
            if (i == 5)
                virtual_goods_cnt_6 = virtual_goods_cnt_keysArr[5] + ":" + virtual_goods_cnt.getString(String.valueOf(virtual_goods_cnt_keysArr[5]));
        }
        String virtual_goods_cnt_sum = virtual_goods_cnt.getString("sum");

        JSONObject virtual_goods_rate = (JSONObject) special_consumption.get("virtual_goods_rate");
        Set<String> virtual_goods_rate_keys = virtual_goods_rate.keySet();
        Object[] virtual_goods_rate_keysArr = virtual_goods_rate_keys.toArray();
        Arrays.sort(virtual_goods_rate_keysArr);
        String virtual_goods_rate_1 = "";
        String virtual_goods_rate_2 = "";
        String virtual_goods_rate_3 = "";
        String virtual_goods_rate_4 = "";
        String virtual_goods_rate_5 = "";
        String virtual_goods_rate_6 = "";
        for (int i = 0; i < virtual_goods_rate_keysArr.length; i++) {
            if (i == 0)
                virtual_goods_rate_1 = virtual_goods_rate_keysArr[0] + ":" + virtual_goods_rate.getString(String.valueOf(virtual_goods_rate_keysArr[0]));
            if (i == 1)
                virtual_goods_rate_2 = virtual_goods_rate_keysArr[1] + ":" + virtual_goods_rate.getString(String.valueOf(virtual_goods_rate_keysArr[1]));
            if (i == 2)
                virtual_goods_rate_3 = virtual_goods_rate_keysArr[2] + ":" + virtual_goods_rate.getString(String.valueOf(virtual_goods_rate_keysArr[2]));
            if (i == 3)
                virtual_goods_rate_4 = virtual_goods_rate_keysArr[3] + ":" + virtual_goods_rate.getString(String.valueOf(virtual_goods_rate_keysArr[3]));
            if (i == 4)
                virtual_goods_rate_5 = virtual_goods_rate_keysArr[4] + ":" + virtual_goods_rate.getString(String.valueOf(virtual_goods_rate_keysArr[4]));
            if (i == 5)
                virtual_goods_rate_6 = virtual_goods_rate_keysArr[5] + ":" + virtual_goods_rate.getString(String.valueOf(virtual_goods_rate_keysArr[5]));
        }
        String virtual_goods_rate_sum = virtual_goods_rate.getString("sum");

        try {
            stmt.setString(101, lottery_amt_1);
            stmt.setString(102, lottery_amt_2);
            stmt.setString(103, lottery_amt_3);
            stmt.setString(104, lottery_amt_4);
            stmt.setString(105, lottery_amt_5);
            stmt.setString(106, lottery_amt_6);
            stmt.setString(107, lottery_amt_sum);
            stmt.setString(108, lottery_cnt_1);
            stmt.setString(109, lottery_cnt_2);
            stmt.setString(110, lottery_cnt_3);
            stmt.setString(111, lottery_cnt_4);
            stmt.setString(112, lottery_cnt_5);
            stmt.setString(113, lottery_cnt_6);
            stmt.setString(114, lottery_cnt_sum);
            stmt.setString(115, lottery_rate_1);
            stmt.setString(116, lottery_rate_2);
            stmt.setString(117, lottery_rate_3);
            stmt.setString(118, lottery_rate_4);
            stmt.setString(119, lottery_rate_5);
            stmt.setString(120, lottery_rate_6);
            stmt.setString(121, lottery_rate_sum);
            stmt.setString(122, virtual_goods_amt_1);
            stmt.setString(123, virtual_goods_amt_2);
            stmt.setString(124, virtual_goods_amt_3);
            stmt.setString(125, virtual_goods_amt_4);
            stmt.setString(126, virtual_goods_amt_5);
            stmt.setString(127, virtual_goods_amt_6);
            stmt.setString(128, virtual_goods_amt_sum);
            stmt.setString(129, virtual_goods_cnt_1);
            stmt.setString(130, virtual_goods_cnt_2);
            stmt.setString(131, virtual_goods_cnt_3);
            stmt.setString(132, virtual_goods_cnt_4);
            stmt.setString(133, virtual_goods_cnt_5);
            stmt.setString(134, virtual_goods_cnt_6);
            stmt.setString(135, virtual_goods_cnt_sum);
            stmt.setString(136, virtual_goods_rate_1);
            stmt.setString(137, virtual_goods_rate_2);
            stmt.setString(138, virtual_goods_rate_3);
            stmt.setString(139, virtual_goods_rate_4);
            stmt.setString(140, virtual_goods_rate_5);
            stmt.setString(141, virtual_goods_rate_6);
            stmt.setString(142, virtual_goods_rate_sum);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void category_one(PreparedStatement stmt, JSONObject consumption_analysis) {
        JSONObject total_consumption = (JSONObject) consumption_analysis.get("total_consumption");
        JSONObject total_category_cnt = (JSONObject) total_consumption.get("total_category_cnt");
        Set<String> total_category_cnt_keys = total_category_cnt.keySet();
        Object[] total_category_cnt_keysArr = total_category_cnt_keys.toArray();
        Arrays.sort(total_category_cnt_keysArr);
        String total_category_cnt_1 = "";
        String total_category_cnt_2 = "";
        String total_category_cnt_3 = "";
        String total_category_cnt_4 = "";
        String total_category_cnt_5 = "";
        String total_category_cnt_6 = "";
        for (int i = total_category_cnt_keysArr.length - 1; i >= 0; i--) {
            if (i == 0)
                total_category_cnt_1 = total_category_cnt_keysArr[0] + ":" + total_category_cnt.getString(String.valueOf(total_category_cnt_keysArr[0]));
            if (i == 1)
                total_category_cnt_2 = total_category_cnt_keysArr[1] + ":" + total_category_cnt.getString(String.valueOf(total_category_cnt_keysArr[1]));
            if (i == 2)
                total_category_cnt_3 = total_category_cnt_keysArr[2] + ":" + total_category_cnt.getString(String.valueOf(total_category_cnt_keysArr[2]));
            if (i == 3)
                total_category_cnt_4 = total_category_cnt_keysArr[3] + ":" + total_category_cnt.getString(String.valueOf(total_category_cnt_keysArr[3]));
            if (i == 4)
                total_category_cnt_5 = total_category_cnt_keysArr[4] + ":" + total_category_cnt.getString(String.valueOf(total_category_cnt_keysArr[4]));
            if (i == 5)
                total_category_cnt_6 = total_category_cnt_keysArr[5] + ":" + total_category_cnt.getString(String.valueOf(total_category_cnt_keysArr[5]));
        }
        String total_category_cnt_sum = total_category_cnt.getString("sum");

        JSONObject total_consum_amt = (JSONObject) total_consumption.get("total_consum_amt");
        Set<String> total_consum_amt_keys = total_consum_amt.keySet();
        Object[] total_consum_amt_keysArr = total_consum_amt_keys.toArray();
        Arrays.sort(total_consum_amt_keysArr);
        String total_consum_amt_1 = "";
        String total_consum_amt_2 = "";
        String total_consum_amt_3 = "";
        String total_consum_amt_4 = "";
        String total_consum_amt_5 = "";
        String total_consum_amt_6 = "";
        for (int i = 0; i < total_consum_amt_keysArr.length; i++) {
            if (i == 0)
                total_consum_amt_1 = total_consum_amt_keysArr[0] + ":" + total_consum_amt.getString(String.valueOf(total_consum_amt_keysArr[0]));
            if (i == 1)
                total_consum_amt_2 = total_consum_amt_keysArr[1] + ":" + total_consum_amt.getString(String.valueOf(total_consum_amt_keysArr[1]));
            if (i == 2)
                total_consum_amt_3 = total_consum_amt_keysArr[2] + ":" + total_consum_amt.getString(String.valueOf(total_consum_amt_keysArr[2]));
            if (i == 3)
                total_consum_amt_4 = total_consum_amt_keysArr[3] + ":" + total_consum_amt.getString(String.valueOf(total_consum_amt_keysArr[3]));
            if (i == 4)
                total_consum_amt_5 = total_consum_amt_keysArr[4] + ":" + total_consum_amt.getString(String.valueOf(total_consum_amt_keysArr[4]));
            if (i == 5)
                total_consum_amt_6 = total_consum_amt_keysArr[5] + ":" + total_consum_amt.getString(String.valueOf(total_consum_amt_keysArr[5]));
        }
        String total_consum_amt_sum = total_consum_amt.getString("sum");

        JSONObject total_consum_times = (JSONObject) total_consumption.get("total_consum_times");
        Set<String> total_consum_times_keys = total_consum_times.keySet();
        Object[] total_consum_times_keysArr = total_consum_times_keys.toArray();
        Arrays.sort(total_consum_times_keysArr);
        String total_consum_times_1 = "";
        String total_consum_times_2 = "";
        String total_consum_times_3 = "";
        String total_consum_times_4 = "";
        String total_consum_times_5 = "";
        String total_consum_times_6 = "";
        for (int i = 0; i < total_consum_times_keysArr.length; i++) {
            if (i == 0)
                total_consum_times_1 = total_consum_times_keysArr[0] + ":" + total_consum_times.getString(String.valueOf(total_consum_times_keysArr[0]));
            if (i == 1)
                total_consum_times_2 = total_consum_times_keysArr[1] + ":" + total_consum_times.getString(String.valueOf(total_consum_times_keysArr[1]));
            if (i == 2)
                total_consum_times_3 = total_consum_times_keysArr[2] + ":" + total_consum_times.getString(String.valueOf(total_consum_times_keysArr[2]));
            if (i == 3)
                total_consum_times_4 = total_consum_times_keysArr[3] + ":" + total_consum_times.getString(String.valueOf(total_consum_times_keysArr[3]));
            if (i == 4)
                total_consum_times_5 = total_consum_times_keysArr[4] + ":" + total_consum_times.getString(String.valueOf(total_consum_times_keysArr[4]));
            if (i == 5)
                total_consum_times_6 = total_consum_times_keysArr[5] + ":" + total_consum_times.getString(String.valueOf(total_consum_times_keysArr[5]));
        }
        String total_consum_times_sum = total_consum_times.getString("sum");

        try {
            stmt.setString(80, total_category_cnt_1);
            stmt.setString(81, total_category_cnt_2);
            stmt.setString(82, total_category_cnt_3);
            stmt.setString(83, total_category_cnt_4);
            stmt.setString(84, total_category_cnt_5);
            stmt.setString(85, total_category_cnt_6);
            stmt.setString(86, total_category_cnt_sum);
            stmt.setString(87, total_consum_amt_1);
            stmt.setString(88, total_consum_amt_2);
            stmt.setString(89, total_consum_amt_3);
            stmt.setString(90, total_consum_amt_4);
            stmt.setString(91, total_consum_amt_5);
            stmt.setString(92, total_consum_amt_6);
            stmt.setString(93, total_consum_amt_sum);
            stmt.setString(94, total_consum_times_1);
            stmt.setString(95, total_consum_times_2);
            stmt.setString(96, total_consum_times_3);
            stmt.setString(97, total_consum_times_4);
            stmt.setString(98, total_consum_times_5);
            stmt.setString(99, total_consum_times_6);
            stmt.setString(100, total_consum_times_sum);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        int files = 0;
        Connection conn = GetConnection.getConn_Cdsp_Riskcore();
        String sql = "";
        sql = "SELECT a.VOUCHER_NO,a.FILE_PATH,r.CLIENT_NO,r.CLIENT_NAME,r.CERT_NO FROM cdsp.cdsp_hulu_access a,riskcore.risk_credit_application r " +
                "WHERE a.QUERY_TYPE = 'MOXIE_TAOBAO_REPORT' AND  a.file_path IS NOT NULL  " +
                "AND a.GMT_CREATED BETWEEN '" + args[0] + "'  AND '" + args[1] + "' AND a.VOUCHER_NO = r.VOUCHER_NO ";
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String voucher_no = rs.getString("VOUCHER_NO");     // 授信编号
                String filePath = rs.getString("FILE_PATH");        // 文件路径
                String client_no = rs.getString("CLIENT_NO");       // 客户编号
                String client_name = rs.getString("CLIENT_NAME");   // 用户姓名
                String cert_no = rs.getString("CERT_NO");           // 身份证号
//                filePath = "D:\\tmp\\taobao_report\\03df568a-9a7c-11e7-a3f0-00163e12d150.json";
                readFileByLines(voucher_no, filePath, client_no, client_name, cert_no);
                System.out.println(DateUtil.nowString() + " ==DS_report=files====" + ++files + "===filepath===" + filePath);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " 电商report导入耗时为： " + (endtime - starttime));
    }


}
