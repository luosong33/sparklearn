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

public class AliplayReport2Phoenix {

    public static void readFileByLines(String voucher_no, String filePath, String client_no, String client_name, String cert_no) {
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
                        //  淘宝用户基本信息
                        JSONObject basic_info = (JSONObject) jobj.get("basic_info");
                        JSONObject user_and_account_basic_info = (JSONObject) basic_info.get("user_and_account_basic_info");
                        String user_name = user_and_account_basic_info.getString("user_name");
                        String card_number = user_and_account_basic_info.getString("card_number");
                        String alipay_gender = user_and_account_basic_info.getString("alipay_gender");
                        String birth_place = user_and_account_basic_info.getString("birth_place");
                        String is_realname = user_and_account_basic_info.getString("is_realname");
                        String alipay_email = user_and_account_basic_info.getString("alipay_email");
                        String phone_number = user_and_account_basic_info.getString("phone_number");
                        String register_month = user_and_account_basic_info.getString("register_month");
                        String total_expenses_amt_6m = user_and_account_basic_info.getString("total_expenses_amt_6m");
                        String total_income_amt_6m = user_and_account_basic_info.getString("total_income_amt_6m");
                        String total_rpy_amt_6m = user_and_account_basic_info.getString("total_rpy_amt_6m");
                        String fund_transe_6m = user_and_account_basic_info.getString("fund_transe_6m");

                        JSONObject wealth_info = (JSONObject) jobj.get("wealth_info");
                        JSONObject total_assets = (JSONObject) wealth_info.get("total_assets");
                        String balance = total_assets.getString("balance");
                        String yu_e_bao = total_assets.getString("yu_e_bao");
                        String zhao_cai_bao = total_assets.getString("zhao_cai_bao");
                        String fund = total_assets.getString("fund");
                        String cun_jin_bao = total_assets.getString("cun_jin_bao");
                        String taobao_finance = total_assets.getString("taobao_finance");
                        String huabai_limit = total_assets.getString("huabai_limit");

                        PreparedStatement stmt = null;
                        Connection con = GetConnection.getPhoenixConn();
                        try {
                            con.setAutoCommit(false);
                            stmt = con.prepareStatement("upsert  into \"alipay_report\"  (\"ID\",\"user_name\",\"card_number\",\"alipay_gender\",\"birth_place\",\"is_realname\",\"alipay_email\",\"phone_number\",\"register_month\",\"total_expenses_amt_6m\",\"total_income_amt_6m\",\"total_rpy_amt_6m\",\"fund_transe_6m\",\"balance\",\"yu_e_bao\",\"zhao_cai_bao\",\"fund\",\"cun_jin_bao\",\"taobao_finance\",\"huabai_limit\",\"credit_rpy_amt_1\",\"credit_rpy_amt_2\",\"credit_rpy_amt_3\",\"credit_rpy_amt_4\",\"credit_rpy_amt_5\",\"credit_rpy_amt_6\",\"credit_rpy_amt_sum\",\"credit_rpy_cnt_1\",\"credit_rpy_cnt_2\",\"credit_rpy_cnt_3\",\"credit_rpy_cnt_4\",\"credit_rpy_cnt_5\",\"credit_rpy_cnt_6\",\"credit_rpy_cnt_sum\",\"huabei_rpy_amt_1\",\"huabei_rpy_amt_2\",\"huabei_rpy_amt_3\",\"huabei_rpy_amt_4\",\"huabei_rpy_amt_5\",\"huabei_rpy_amt_6\",\"huabei_rpy_amt_sum\",\"huabei_rpy_cnt_1\",\"huabei_rpy_cnt_2\",\"huabei_rpy_cnt_3\",\"huabei_rpy_cnt_4\",\"huabei_rpy_cnt_5\",\"huabei_rpy_cnt_6\",\"huabei_rpy_cnt_sum\",\"jiebei_rpy_amt_1\",\"jiebei_rpy_amt_2\",\"jiebei_rpy_amt_3\",\"jiebei_rpy_amt_4\",\"jiebei_rpy_amt_5\",\"jiebei_rpy_amt_6\",\"jiebei_rpy_amt_sum\",\"jiebei_rpy_cnt_1\",\"jiebei_rpy_cnt_2\",\"jiebei_rpy_cnt_3\",\"jiebei_rpy_cnt_4\",\"jiebei_rpy_cnt_5\",\"jiebei_rpy_cnt_6\",\"jiebei_rpy_cnt_sum\",\"other_rpy_amt_1\",\"other_rpy_amt_2\",\"other_rpy_amt_3\",\"other_rpy_amt_4\",\"other_rpy_amt_5\",\"other_rpy_amt_6\",\"other_rpy_amt_sum\",\"other_rpy_cnt_1\",\"other_rpy_cnt_2\",\"other_rpy_cnt_3\",\"other_rpy_cnt_4\",\"other_rpy_cnt_5\",\"other_rpy_cnt_6\",\"other_rpy_cnt_sum\",\"total_consume_amt_1\",\"total_consume_amt_2\",\"total_consume_amt_3\",\"total_consume_amt_4\",\"total_consume_amt_5\",\"total_consume_amt_6\",\"total_consume_amt_sum\",\"total_consume_cnt_1\",\"total_consume_cnt_2\",\"total_consume_cnt_3\",\"total_consume_cnt_4\",\"total_consume_cnt_5\",\"total_consume_cnt_6\",\"total_consume_cnt_sum\",\"max_consume_amt_1\",\"max_consume_amt_2\",\"max_consume_amt_3\",\"max_consume_amt_4\",\"max_consume_amt_5\",\"max_consume_amt_6\",\"max_consume_amt_sum\",\"online_shopping_amt_1\",\"online_shopping_amt_2\",\"online_shopping_amt_3\",\"online_shopping_amt_4\",\"online_shopping_amt_5\",\"online_shopping_amt_6\",\"online_shopping_amt_sum\",\"online_shopping_cnt_1\",\"online_shopping_cnt_2\",\"online_shopping_cnt_3\",\"online_shopping_cnt_4\",\"online_shopping_cnt_5\",\"online_shopping_cnt_6\",\"online_shopping_cnt_sum\",\"takeout_amt_1\",\"takeout_amt_2\",\"takeout_amt_3\",\"takeout_amt_4\",\"takeout_amt_5\",\"takeout_amt_6\",\"takeout_amt_sum\",\"takeout_cnt_1\",\"takeout_cnt_2\",\"takeout_cnt_3\",\"takeout_cnt_4\",\"takeout_cnt_5\",\"takeout_cnt_6\",\"takeout_cnt_sum\",\"lifepay_amt_1\",\"lifepay_amt_2\",\"lifepay_amt_3\",\"lifepay_amt_4\",\"lifepay_amt_5\",\"lifepay_amt_6\",\"lifepay_amt_sum\",\"lifepay_cnt_1\",\"lifepay_cnt_2\",\"lifepay_cnt_3\",\"lifepay_cnt_4\",\"lifepay_cnt_5\",\"lifepay_cnt_6\",\"lifepay_cnt_sum\",\"taxipay_amt_1\",\"taxipay_amt_2\",\"taxipay_amt_3\",\"taxipay_amt_4\",\"taxipay_amt_5\",\"taxipay_amt_6\",\"taxipay_amt_sum\",\"taxipay_cnt_1\",\"taxipay_cnt_2\",\"taxipay_cnt_3\",\"taxipay_cnt_4\",\"taxipay_cnt_5\",\"taxipay_cnt_6\",\"taxipay_cnt_sum\",\"carpay_amt_1\",\"carpay_amt_2\",\"carpay_amt_3\",\"carpay_amt_4\",\"carpay_amt_5\",\"carpay_amt_6\",\"carpay_amt_sum\",\"carpay_cnt_1\",\"carpay_cnt_2\",\"carpay_cnt_3\",\"carpay_cnt_4\",\"carpay_cnt_5\",\"carpay_cnt_6\",\"carpay_cnt_sum\",\"travel_amt_1\",\"travel_amt_2\",\"travel_amt_3\",\"travel_amt_4\",\"travel_amt_5\",\"travel_amt_6\",\"travel_amt_sum\",\"travel_cnt_1\",\"travel_cnt_2\",\"travel_cnt_3\",\"travel_cnt_4\",\"travel_cnt_5\",\"travel_cnt_6\",\"travel_cnt_sum\",\"lottery_amt_1\",\"lottery_amt_2\",\"lottery_amt_3\",\"lottery_amt_4\",\"lottery_amt_5\",\"lottery_amt_6\",\"lottery_amt_sum\",\"lottery_rate_1\",\"lottery_rate_2\",\"lottery_rate_3\",\"lottery_rate_4\",\"lottery_rate_5\",\"lottery_rate_6\",\"lottery_rate_sum\",\"lottery_cnt_1\",\"lottery_cnt_2\",\"lottery_cnt_3\",\"lottery_cnt_4\",\"lottery_cnt_5\",\"lottery_cnt_6\",\"lottery_cnt_sum\",\"game_amt_1\",\"game_amt_2\",\"game_amt_3\",\"game_amt_4\",\"game_amt_5\",\"game_amt_6\",\"game_amt_sum\",\"game_rate_1\",\"game_rate_2\",\"game_rate_3\",\"game_rate_4\",\"game_rate_5\",\"game_rate_6\",\"game_rate_sum\",\"game_cnt_1\",\"game_cnt_2\",\"game_cnt_3\",\"game_cnt_4\",\"game_cnt_5\",\"game_cnt_6\",\"game_cnt_sum\",\"out_amt_1\",\"out_amt_2\",\"out_amt_3\",\"out_amt_4\",\"out_amt_5\",\"out_amt_6\",\"out_amt_sum\",\"out_cnt_1\",\"out_cnt_2\",\"out_cnt_3\",\"out_cnt_4\",\"out_cnt_5\",\"out_cnt_6\",\"out_cnt_sum\",\"max_out_amt_1\",\"max_out_amt_2\",\"max_out_amt_3\",\"max_out_amt_4\",\"max_out_amt_5\",\"max_out_amt_6\",\"max_out_amt_sum\",\"zhao_cai_bao_purchase_amt_1\",\"zhao_cai_bao_purchase_amt_2\",\"zhao_cai_bao_purchase_amt_3\",\"zhao_cai_bao_purchase_amt_4\",\"zhao_cai_bao_purchase_amt_5\",\"zhao_cai_bao_purchase_amt_6\",\"zhao_cai_bao_purchase_amt_sum\",\"fund_purchase_amt_1\",\"fund_purchase_amt_2\",\"fund_purchase_amt_3\",\"fund_purchase_amt_4\",\"fund_purchase_amt_5\",\"fund_purchase_amt_6\",\"fund_purchase_amt_sum\",\"cun_jin_bao_purchase_amt_1\",\"cun_jin_bao_purchase_amt_2\",\"cun_jin_bao_purchase_amt_3\",\"cun_jin_bao_purchase_amt_4\",\"cun_jin_bao_purchase_amt_5\",\"cun_jin_bao_purchase_amt_6\",\"cun_jin_bao_purchase_amt_sum\",\"redpkt_amt_1\",\"redpkt_amt_2\",\"redpkt_amt_3\",\"redpkt_amt_4\",\"redpkt_amt_5\",\"redpkt_amt_6\",\"redpkt_amt_sum\",\"redpkt_cnt_1\",\"redpkt_cnt_2\",\"redpkt_cnt_3\",\"redpkt_cnt_4\",\"redpkt_cnt_5\",\"redpkt_cnt_6\",\"redpkt_cnt_sum\",\"max_redpkt_amt_1\",\"max_redpkt_amt_2\",\"max_redpkt_amt_3\",\"max_redpkt_amt_4\",\"max_redpkt_amt_5\",\"max_redpkt_amt_6\",\"max_redpkt_amt_sum\",\"donate_amt_1\",\"donate_amt_2\",\"donate_amt_3\",\"donate_amt_4\",\"donate_amt_5\",\"donate_amt_6\",\"donate_amt_sum\",\"donate_cnt_1\",\"donate_cnt_2\",\"donate_cnt_3\",\"donate_cnt_4\",\"donate_cnt_5\",\"donate_cnt_6\",\"donate_cnt_sum\",\"gratuity_amt_1\",\"gratuity_amt_2\",\"gratuity_amt_3\",\"gratuity_amt_4\",\"gratuity_amt_5\",\"gratuity_amt_6\",\"gratuity_amt_sum\",\"gratuity_cnt_1\",\"gratuity_cnt_2\",\"gratuity_cnt_3\",\"gratuity_cnt_4\",\"gratuity_cnt_5\",\"gratuity_cnt_6\",\"gratuity_cnt_sum\",\"pay_for_ohter_amt_1\",\"pay_for_ohter_amt_2\",\"pay_for_ohter_amt_3\",\"pay_for_ohter_amt_4\",\"pay_for_ohter_amt_5\",\"pay_for_ohter_amt_6\",\"pay_for_ohter_amt_sum\",\"pay_for_ohter_cnt_1\",\"pay_for_ohter_cnt_2\",\"pay_for_ohter_cnt_3\",\"pay_for_ohter_cnt_4\",\"pay_for_ohter_cnt_5\",\"pay_for_ohter_cnt_6\",\"pay_for_ohter_cnt_sum\",\"jiebei_loan_amt_1\",\"jiebei_loan_amt_2\",\"jiebei_loan_amt_3\",\"jiebei_loan_amt_4\",\"jiebei_loan_amt_5\",\"jiebei_loan_amt_6\",\"jiebei_loan_amt_sum\",\"jiebei_loan_cnt_1\",\"jiebei_loan_cnt_2\",\"jiebei_loan_cnt_3\",\"jiebei_loan_cnt_4\",\"jiebei_loan_cnt_5\",\"jiebei_loan_cnt_6\",\"jiebei_loan_cnt_sum\",\"other_loan_amt_1\",\"other_loan_amt_2\",\"other_loan_amt_3\",\"other_loan_amt_4\",\"other_loan_amt_5\",\"other_loan_amt_6\",\"other_loan_amt_sum\",\"other_loan_cnt_1\",\"other_loan_cnt_2\",\"other_loan_cnt_3\",\"other_loan_cnt_4\",\"other_loan_cnt_5\",\"other_loan_cnt_6\",\"other_loan_cnt_sum\",\"transfer_in_amt_1\",\"transfer_in_amt_2\",\"transfer_in_amt_3\",\"transfer_in_amt_4\",\"transfer_in_amt_5\",\"transfer_in_amt_6\",\"transfer_in_amt_sum\",\"transfer_in_cnt_1\",\"transfer_in_cnt_2\",\"transfer_in_cnt_3\",\"transfer_in_cnt_4\",\"transfer_in_cnt_5\",\"transfer_in_cnt_6\",\"transfer_in_cnt_sum\",\"max_transfer_in_amt_1\",\"max_transfer_in_amt_2\",\"max_transfer_in_amt_3\",\"max_transfer_in_amt_4\",\"max_transfer_in_amt_5\",\"max_transfer_in_amt_6\",\"max_transfer_in_amt_sum\",\"refund_amt_1\",\"refund_amt_2\",\"refund_amt_3\",\"refund_amt_4\",\"refund_amt_5\",\"refund_amt_6\",\"refund_amt_sum\",\"refund_cnt_1\",\"refund_cnt_2\",\"refund_cnt_3\",\"refund_cnt_4\",\"refund_cnt_5\",\"refund_cnt_6\",\"refund_cnt_sum\",\"max_refund_amt_1\",\"max_refund_amt_2\",\"max_refund_amt_3\",\"max_refund_amt_4\",\"max_refund_amt_5\",\"max_refund_amt_6\",\"max_refund_amt_sum\",\"yu_e_bao_profit_amt_1\",\"yu_e_bao_profit_amt_2\",\"yu_e_bao_profit_amt_3\",\"yu_e_bao_profit_amt_4\",\"yu_e_bao_profit_amt_5\",\"yu_e_bao_profit_amt_6\",\"yu_e_bao_profit_amt_sum\",\"zhao_cai_bao_redeem_amt_1\",\"zhao_cai_bao_redeem_amt_2\",\"zhao_cai_bao_redeem_amt_3\",\"zhao_cai_bao_redeem_amt_4\",\"zhao_cai_bao_redeem_amt_5\",\"zhao_cai_bao_redeem_amt_6\",\"zhao_cai_bao_redeem_amt_sum\",\"zhao_cai_bao_redeem_cnt_1\",\"zhao_cai_bao_redeem_cnt_2\",\"zhao_cai_bao_redeem_cnt_3\",\"zhao_cai_bao_redeem_cnt_4\",\"zhao_cai_bao_redeem_cnt_5\",\"zhao_cai_bao_redeem_cnt_6\",\"zhao_cai_bao_redeem_cnt_sum\",\"fund_redeem_amt_1\",\"fund_redeem_amt_2\",\"fund_redeem_amt_3\",\"fund_redeem_amt_4\",\"fund_redeem_amt_5\",\"fund_redeem_amt_6\",\"fund_redeem_amt_sum\",\"fund_redeem_cnt_1\",\"fund_redeem_cnt_2\",\"fund_redeem_cnt_3\",\"fund_redeem_cnt_4\",\"fund_redeem_cnt_5\",\"fund_redeem_cnt_6\",\"fund_redeem_cnt_sum\",\"cun_jin_bao_redeem_amt_1\",\"cun_jin_bao_redeem_amt_2\",\"cun_jin_bao_redeem_amt_3\",\"cun_jin_bao_redeem_amt_4\",\"cun_jin_bao_redeem_amt_5\",\"cun_jin_bao_redeem_amt_6\",\"cun_jin_bao_redeem_amt_sum\",\"cun_jin_bao_redeem_cnt_1\",\"cun_jin_bao_redeem_cnt_2\",\"cun_jin_bao_redeem_cnt_3\",\"cun_jin_bao_redeem_cnt_4\",\"cun_jin_bao_redeem_cnt_5\",\"cun_jin_bao_redeem_cnt_6\",\"cun_jin_bao_redeem_cnt_sum\",\"loadtime\",\"client_name\",\"cert_no\") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                            stmt.setString(1, client_no+"_"+voucher_no);
                            stmt.setString(2, user_name != null ? user_name : "");
                            stmt.setString(3, card_number != null ? card_number : "");
                            stmt.setString(4, alipay_gender != null ? alipay_gender : "");
                            stmt.setString(5, birth_place != null ? birth_place : "");
                            stmt.setString(6, is_realname != null ? is_realname : "");
                            stmt.setString(7, alipay_email != null ? alipay_email : "");
                            stmt.setString(8, phone_number != null ? phone_number : "");
                            stmt.setString(9, register_month != null ? register_month : "");
                            stmt.setString(10, total_expenses_amt_6m != null ? total_expenses_amt_6m : "");
                            stmt.setString(11, total_income_amt_6m != null ? total_income_amt_6m : "");
                            stmt.setString(12, total_rpy_amt_6m != null ? total_rpy_amt_6m : "");
                            stmt.setString(13, fund_transe_6m != null ? fund_transe_6m : "");
                            stmt.setString(14, balance != null ? balance : "");
                            stmt.setString(15, yu_e_bao != null ? yu_e_bao : "");
                            stmt.setString(16, zhao_cai_bao != null ? zhao_cai_bao : "");
                            stmt.setString(17, fund != null ? fund : "");
                            stmt.setString(18, cun_jin_bao != null ? cun_jin_bao : "");
                            stmt.setString(19, taobao_finance != null ? taobao_finance : "");
                            stmt.setString(20, huabai_limit != null ? huabai_limit : "");

                            stmt.setString(448, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                            stmt.setString(449, client_name);
                            stmt.setString(450, cert_no);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                        //  类目一
                        JSONObject major_expenditure = (JSONObject) jobj.get("major_expenditure");
                        category_one(stmt, major_expenditure);
                        // 类目二
                        JSONObject infolow_of_capital = (JSONObject) jobj.get("infolow_of_capital");
                        category_two(stmt, infolow_of_capital);

                        try {
                            stmt.addBatch();
                            stmt.executeBatch();
//                            stmt.executeUpdate();
                            con.commit();
                            con.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
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

    private static void category_two(PreparedStatement stmt, JSONObject infolow_of_capital) {
        //  1
        JSONObject borrowed_funds = (JSONObject) infolow_of_capital.get("borrowed_funds");
        JSONObject jiebei_loan_amt = (JSONObject) borrowed_funds.get("jiebei_loan_amt");
        Set<String> jiebei_loan_amt_keys = jiebei_loan_amt.keySet();
        Object[] jiebei_loan_amt_keysArr = jiebei_loan_amt_keys.toArray();
        Arrays.sort(jiebei_loan_amt_keysArr);
        String jiebei_loan_amt_1 = "";
        String jiebei_loan_amt_2 = "";
        String jiebei_loan_amt_3 = "";
        String jiebei_loan_amt_4 = "";
        String jiebei_loan_amt_5 = "";
        String jiebei_loan_amt_6 = "";
        for (int i = 0; i < jiebei_loan_amt_keysArr.length; i++ ){
            if (i == 0) jiebei_loan_amt_1 = jiebei_loan_amt.getString(String.valueOf(jiebei_loan_amt_keysArr[0]));
            if (i == 1) jiebei_loan_amt_2 = jiebei_loan_amt.getString(String.valueOf(jiebei_loan_amt_keysArr[1]));
            if (i == 2) jiebei_loan_amt_3 = jiebei_loan_amt.getString(String.valueOf(jiebei_loan_amt_keysArr[2]));
            if (i == 3) jiebei_loan_amt_4 = jiebei_loan_amt.getString(String.valueOf(jiebei_loan_amt_keysArr[3]));
            if (i == 4) jiebei_loan_amt_5 = jiebei_loan_amt.getString(String.valueOf(jiebei_loan_amt_keysArr[4]));
            if (i == 5) jiebei_loan_amt_6 = jiebei_loan_amt.getString(String.valueOf(jiebei_loan_amt_keysArr[5]));
        }
        String jiebei_loan_amt_sum = jiebei_loan_amt.getString("sum");

        JSONObject jiebei_loan_cnt = (JSONObject) borrowed_funds.get("jiebei_loan_cnt");
        Set<String> jiebei_loan_cnt_keys = jiebei_loan_cnt.keySet();
        Object[] jiebei_loan_cnt_keysArr = jiebei_loan_cnt_keys.toArray();
        Arrays.sort(jiebei_loan_cnt_keysArr);
        String jiebei_loan_cnt_1 = "";
        String jiebei_loan_cnt_2 = "";
        String jiebei_loan_cnt_3 = "";
        String jiebei_loan_cnt_4 = "";
        String jiebei_loan_cnt_5 = "";
        String jiebei_loan_cnt_6 = "";
        for (int i = 0; i < jiebei_loan_cnt_keysArr.length; i++ ){
            if (i == 0) jiebei_loan_cnt_1 = jiebei_loan_cnt.getString(String.valueOf(jiebei_loan_cnt_keysArr[0]));
            if (i == 1) jiebei_loan_cnt_2 = jiebei_loan_cnt.getString(String.valueOf(jiebei_loan_cnt_keysArr[1]));
            if (i == 2) jiebei_loan_cnt_3 = jiebei_loan_cnt.getString(String.valueOf(jiebei_loan_cnt_keysArr[2]));
            if (i == 3) jiebei_loan_cnt_4 = jiebei_loan_cnt.getString(String.valueOf(jiebei_loan_cnt_keysArr[3]));
            if (i == 4) jiebei_loan_cnt_5 = jiebei_loan_cnt.getString(String.valueOf(jiebei_loan_cnt_keysArr[4]));
            if (i == 5) jiebei_loan_cnt_6 = jiebei_loan_cnt.getString(String.valueOf(jiebei_loan_cnt_keysArr[5]));
        }
        String jiebei_loan_cnt_sum = jiebei_loan_cnt.getString("sum");

        JSONObject other_loan_amt = (JSONObject) borrowed_funds.get("other_loan_amt");
        Set<String> other_loan_amt_keys = other_loan_amt.keySet();
        Object[] other_loan_amt_keysArr = other_loan_amt_keys.toArray();
        Arrays.sort(other_loan_amt_keysArr);
        String other_loan_amt_1 = "";
        String other_loan_amt_2 = "";
        String other_loan_amt_3 = "";
        String other_loan_amt_4 = "";
        String other_loan_amt_5 = "";
        String other_loan_amt_6 = "";
        for (int i = 0; i < other_loan_amt_keysArr.length; i++ ){
            if (i == 0) other_loan_amt_1 = other_loan_amt.getString(String.valueOf(other_loan_amt_keysArr[0]));
            if (i == 1) other_loan_amt_2 = other_loan_amt.getString(String.valueOf(other_loan_amt_keysArr[1]));
            if (i == 2) other_loan_amt_3 = other_loan_amt.getString(String.valueOf(other_loan_amt_keysArr[2]));
            if (i == 3) other_loan_amt_4 = other_loan_amt.getString(String.valueOf(other_loan_amt_keysArr[3]));
            if (i == 4) other_loan_amt_5 = other_loan_amt.getString(String.valueOf(other_loan_amt_keysArr[4]));
            if (i == 5) other_loan_amt_6 = other_loan_amt.getString(String.valueOf(other_loan_amt_keysArr[5]));
        }
        String other_loan_amt_sum = other_loan_amt.getString("sum");

        JSONObject other_loan_cnt = (JSONObject) borrowed_funds.get("other_loan_cnt");
        Set<String> other_loan_cnt_keys = other_loan_cnt.keySet();
        Object[] other_loan_cnt_keysArr = other_loan_cnt_keys.toArray();
        Arrays.sort(other_loan_cnt_keysArr);
        String other_loan_cnt_1 = "";
        String other_loan_cnt_2 = "";
        String other_loan_cnt_3 = "";
        String other_loan_cnt_4 = "";
        String other_loan_cnt_5 = "";
        String other_loan_cnt_6 = "";
        for (int i = 0; i < other_loan_cnt_keysArr.length; i++ ){
            if (i == 0) other_loan_cnt_1 = other_loan_cnt.getString(String.valueOf(other_loan_cnt_keysArr[0]));
            if (i == 1) other_loan_cnt_2 = other_loan_cnt.getString(String.valueOf(other_loan_cnt_keysArr[1]));
            if (i == 2) other_loan_cnt_3 = other_loan_cnt.getString(String.valueOf(other_loan_cnt_keysArr[2]));
            if (i == 3) other_loan_cnt_4 = other_loan_cnt.getString(String.valueOf(other_loan_cnt_keysArr[3]));
            if (i == 4) other_loan_cnt_5 = other_loan_cnt.getString(String.valueOf(other_loan_cnt_keysArr[4]));
            if (i == 5) other_loan_cnt_6 = other_loan_cnt.getString(String.valueOf(other_loan_cnt_keysArr[5]));
        }
        String other_loan_cnt_sum = other_loan_cnt.getString("sum");

        //  2
        JSONObject transfer_in_info = (JSONObject) infolow_of_capital.get("transfer_in_info");
        JSONObject transfer_in_amt = (JSONObject) transfer_in_info.get("transfer_in_amt");
        Set<String> transfer_in_amt_keys = transfer_in_amt.keySet();
        Object[] transfer_in_amt_keysArr = transfer_in_amt_keys.toArray();
        Arrays.sort(transfer_in_amt_keysArr);
        String transfer_in_amt_1 = "";
        String transfer_in_amt_2 = "";
        String transfer_in_amt_3 = "";
        String transfer_in_amt_4 = "";
        String transfer_in_amt_5 = "";
        String transfer_in_amt_6 = "";
        for (int i = 0; i < transfer_in_amt_keysArr.length; i++ ){
            if (i == 0) transfer_in_amt_1 = transfer_in_amt.getString(String.valueOf(transfer_in_amt_keysArr[0]));
            if (i == 1) transfer_in_amt_2 = transfer_in_amt.getString(String.valueOf(transfer_in_amt_keysArr[1]));
            if (i == 2) transfer_in_amt_3 = transfer_in_amt.getString(String.valueOf(transfer_in_amt_keysArr[2]));
            if (i == 3) transfer_in_amt_4 = transfer_in_amt.getString(String.valueOf(transfer_in_amt_keysArr[3]));
            if (i == 4) transfer_in_amt_5 = transfer_in_amt.getString(String.valueOf(transfer_in_amt_keysArr[4]));
            if (i == 5) transfer_in_amt_6 = transfer_in_amt.getString(String.valueOf(transfer_in_amt_keysArr[5]));
        }
        String transfer_in_amt_sum = transfer_in_amt.getString("sum");

        JSONObject transfer_in_cnt = (JSONObject) transfer_in_info.get("transfer_in_cnt");
        Set<String> transfer_in_cnt_keys = transfer_in_cnt.keySet();
        Object[] transfer_in_cnt_keysArr = transfer_in_cnt_keys.toArray();
        Arrays.sort(transfer_in_cnt_keysArr);
        String transfer_in_cnt_1 = "";
        String transfer_in_cnt_2 = "";
        String transfer_in_cnt_3 = "";
        String transfer_in_cnt_4 = "";
        String transfer_in_cnt_5 = "";
        String transfer_in_cnt_6 = "";
        for (int i = 0; i < transfer_in_cnt_keysArr.length; i++ ){
            if (i == 0) transfer_in_cnt_1 = transfer_in_cnt.getString(String.valueOf(transfer_in_cnt_keysArr[0]));
            if (i == 1) transfer_in_cnt_2 = transfer_in_cnt.getString(String.valueOf(transfer_in_cnt_keysArr[1]));
            if (i == 2) transfer_in_cnt_3 = transfer_in_cnt.getString(String.valueOf(transfer_in_cnt_keysArr[2]));
            if (i == 3) transfer_in_cnt_4 = transfer_in_cnt.getString(String.valueOf(transfer_in_cnt_keysArr[3]));
            if (i == 4) transfer_in_cnt_5 = transfer_in_cnt.getString(String.valueOf(transfer_in_cnt_keysArr[4]));
            if (i == 5) transfer_in_cnt_6 = transfer_in_cnt.getString(String.valueOf(transfer_in_cnt_keysArr[5]));
        }
        String transfer_in_cnt_sum = transfer_in_cnt.getString("sum");

        JSONObject max_transfer_in_amt = (JSONObject) transfer_in_info.get("max_transfer_in_amt");
        Set<String> max_transfer_in_amt_keys = max_transfer_in_amt.keySet();
        Object[] max_transfer_in_amt_keysArr = max_transfer_in_amt_keys.toArray();
        Arrays.sort(max_transfer_in_amt_keysArr);
        String max_transfer_in_amt_1 = "";
        String max_transfer_in_amt_2 = "";
        String max_transfer_in_amt_3 = "";
        String max_transfer_in_amt_4 = "";
        String max_transfer_in_amt_5 = "";
        String max_transfer_in_amt_6 = "";
        for (int i = 0; i < max_transfer_in_amt_keysArr.length; i++ ){
            if (i == 0) max_transfer_in_amt_1 = max_transfer_in_amt.getString(String.valueOf(max_transfer_in_amt_keysArr[0]));
            if (i == 1) max_transfer_in_amt_2 = max_transfer_in_amt.getString(String.valueOf(max_transfer_in_amt_keysArr[1]));
            if (i == 2) max_transfer_in_amt_3 = max_transfer_in_amt.getString(String.valueOf(max_transfer_in_amt_keysArr[2]));
            if (i == 3) max_transfer_in_amt_4 = max_transfer_in_amt.getString(String.valueOf(max_transfer_in_amt_keysArr[3]));
            if (i == 4) max_transfer_in_amt_5 = max_transfer_in_amt.getString(String.valueOf(max_transfer_in_amt_keysArr[4]));
            if (i == 5) max_transfer_in_amt_6 = max_transfer_in_amt.getString(String.valueOf(max_transfer_in_amt_keysArr[5]));
        }
        String max_transfer_in_amt_sum = max_transfer_in_amt.getString("sum");

        //  3
        JSONObject refund_amount = (JSONObject) infolow_of_capital.get("refund_amount");
        JSONObject refund_amt = (JSONObject) refund_amount.get("refund_amt");
        Set<String> refund_amt_keys = refund_amt.keySet();
        Object[] refund_amt_keysArr = refund_amt_keys.toArray();
        Arrays.sort(refund_amt_keysArr);
        String refund_amt_1 = "";
        String refund_amt_2 = "";
        String refund_amt_3 = "";
        String refund_amt_4 = "";
        String refund_amt_5 = "";
        String refund_amt_6 = "";
        for (int i = 0; i < refund_amt_keysArr.length; i++ ){
            if (i == 0) refund_amt_1 = refund_amt.getString(String.valueOf(refund_amt_keysArr[0]));
            if (i == 1) refund_amt_2 = refund_amt.getString(String.valueOf(refund_amt_keysArr[1]));
            if (i == 2) refund_amt_3 = refund_amt.getString(String.valueOf(refund_amt_keysArr[2]));
            if (i == 3) refund_amt_4 = refund_amt.getString(String.valueOf(refund_amt_keysArr[3]));
            if (i == 4) refund_amt_5 = refund_amt.getString(String.valueOf(refund_amt_keysArr[4]));
            if (i == 5) refund_amt_6 = refund_amt.getString(String.valueOf(refund_amt_keysArr[5]));
        }
        String refund_amt_sum = refund_amt.getString("sum");

        JSONObject refund_cnt = (JSONObject) refund_amount.get("refund_cnt");
        Set<String> refund_cnt_keys = refund_cnt.keySet();
        Object[] refund_cnt_keysArr = refund_cnt_keys.toArray();
        Arrays.sort(refund_cnt_keysArr);
        String refund_cnt_1 = "";
        String refund_cnt_2 = "";
        String refund_cnt_3 = "";
        String refund_cnt_4 = "";
        String refund_cnt_5 = "";
        String refund_cnt_6 = "";
        for (int i = 0; i < refund_cnt_keysArr.length; i++ ){
            if (i == 0) refund_cnt_1 = refund_cnt.getString(String.valueOf(refund_cnt_keysArr[0]));
            if (i == 1) refund_cnt_2 = refund_cnt.getString(String.valueOf(refund_cnt_keysArr[1]));
            if (i == 2) refund_cnt_3 = refund_cnt.getString(String.valueOf(refund_cnt_keysArr[2]));
            if (i == 3) refund_cnt_4 = refund_cnt.getString(String.valueOf(refund_cnt_keysArr[3]));
            if (i == 4) refund_cnt_5 = refund_cnt.getString(String.valueOf(refund_cnt_keysArr[4]));
            if (i == 5) refund_cnt_6 = refund_cnt.getString(String.valueOf(refund_cnt_keysArr[5]));
        }
        String refund_cnt_sum = refund_cnt.getString("sum");

        JSONObject max_refund_amt = (JSONObject) refund_amount.get("max_refund_amt");
        Set<String> max_refund_amt_keys = max_refund_amt.keySet();
        Object[] max_refund_amt_keysArr = max_refund_amt_keys.toArray();
        Arrays.sort(max_refund_amt_keysArr);
        String max_refund_amt_1 = "";
        String max_refund_amt_2 = "";
        String max_refund_amt_3 = "";
        String max_refund_amt_4 = "";
        String max_refund_amt_5 = "";
        String max_refund_amt_6 = "";
        for (int i = 0; i < max_refund_amt_keysArr.length; i++ ){
            if (i == 0) max_refund_amt_1 = max_refund_amt.getString(String.valueOf(max_refund_amt_keysArr[0]));
            if (i == 1) max_refund_amt_2 = max_refund_amt.getString(String.valueOf(max_refund_amt_keysArr[1]));
            if (i == 2) max_refund_amt_3 = max_refund_amt.getString(String.valueOf(max_refund_amt_keysArr[2]));
            if (i == 3) max_refund_amt_4 = max_refund_amt.getString(String.valueOf(max_refund_amt_keysArr[3]));
            if (i == 4) max_refund_amt_5 = max_refund_amt.getString(String.valueOf(max_refund_amt_keysArr[4]));
            if (i == 5) max_refund_amt_6 = max_refund_amt.getString(String.valueOf(max_refund_amt_keysArr[5]));
        }
        String max_refund_amt_sum = max_refund_amt.getString("sum");

        //  4
        JSONObject other_in_com = (JSONObject) infolow_of_capital.get("other_in_com");
        JSONObject yu_e_bao_profit_amt = (JSONObject) other_in_com.get("yu_e_bao_profit_amt");
        Set<String> yu_e_bao_profit_amt_keys = yu_e_bao_profit_amt.keySet();
        Object[] yu_e_bao_profit_amt_keysArr = yu_e_bao_profit_amt_keys.toArray();
        Arrays.sort(yu_e_bao_profit_amt_keysArr);
        String yu_e_bao_profit_amt_1 = "";
        String yu_e_bao_profit_amt_2 = "";
        String yu_e_bao_profit_amt_3 = "";
        String yu_e_bao_profit_amt_4 = "";
        String yu_e_bao_profit_amt_5 = "";
        String yu_e_bao_profit_amt_6 = "";
        for (int i = 0; i < yu_e_bao_profit_amt_keysArr.length; i++ ){
            if (i == 0) yu_e_bao_profit_amt_1 = yu_e_bao_profit_amt.getString(String.valueOf(yu_e_bao_profit_amt_keysArr[0]));
            if (i == 1) yu_e_bao_profit_amt_2 = yu_e_bao_profit_amt.getString(String.valueOf(yu_e_bao_profit_amt_keysArr[1]));
            if (i == 2) yu_e_bao_profit_amt_3 = yu_e_bao_profit_amt.getString(String.valueOf(yu_e_bao_profit_amt_keysArr[2]));
            if (i == 3) yu_e_bao_profit_amt_4 = yu_e_bao_profit_amt.getString(String.valueOf(yu_e_bao_profit_amt_keysArr[3]));
            if (i == 4) yu_e_bao_profit_amt_5 = yu_e_bao_profit_amt.getString(String.valueOf(yu_e_bao_profit_amt_keysArr[4]));
            if (i == 5) yu_e_bao_profit_amt_6 = yu_e_bao_profit_amt.getString(String.valueOf(yu_e_bao_profit_amt_keysArr[5]));
        }
        String yu_e_bao_profit_amt_sum = yu_e_bao_profit_amt.getString("sum");

        JSONObject zhao_cai_bao_redeem_amt = (JSONObject) other_in_com.get("zhao_cai_bao_redeem_amt");
        Set<String> zhao_cai_bao_redeem_amt_keys = zhao_cai_bao_redeem_amt.keySet();
        Object[] zhao_cai_bao_redeem_amt_keysArr = zhao_cai_bao_redeem_amt_keys.toArray();
        Arrays.sort(zhao_cai_bao_redeem_amt_keysArr);
        String zhao_cai_bao_redeem_amt_1 = "";
        String zhao_cai_bao_redeem_amt_2 = "";
        String zhao_cai_bao_redeem_amt_3 = "";
        String zhao_cai_bao_redeem_amt_4 = "";
        String zhao_cai_bao_redeem_amt_5 = "";
        String zhao_cai_bao_redeem_amt_6 = "";
        for (int i = 0; i < zhao_cai_bao_redeem_amt_keysArr.length; i++ ){
            if (i == 0) zhao_cai_bao_redeem_amt_1 = zhao_cai_bao_redeem_amt.getString(String.valueOf(zhao_cai_bao_redeem_amt_keysArr[0]));
            if (i == 1) zhao_cai_bao_redeem_amt_2 = zhao_cai_bao_redeem_amt.getString(String.valueOf(zhao_cai_bao_redeem_amt_keysArr[1]));
            if (i == 2) zhao_cai_bao_redeem_amt_3 = zhao_cai_bao_redeem_amt.getString(String.valueOf(zhao_cai_bao_redeem_amt_keysArr[2]));
            if (i == 3) zhao_cai_bao_redeem_amt_4 = zhao_cai_bao_redeem_amt.getString(String.valueOf(zhao_cai_bao_redeem_amt_keysArr[3]));
            if (i == 4) zhao_cai_bao_redeem_amt_5 = zhao_cai_bao_redeem_amt.getString(String.valueOf(zhao_cai_bao_redeem_amt_keysArr[4]));
            if (i == 5) zhao_cai_bao_redeem_amt_6 = zhao_cai_bao_redeem_amt.getString(String.valueOf(zhao_cai_bao_redeem_amt_keysArr[5]));
        }
        String zhao_cai_bao_redeem_amt_sum = zhao_cai_bao_redeem_amt.getString("sum");

        JSONObject zhao_cai_bao_redeem_cnt = (JSONObject) other_in_com.get("zhao_cai_bao_redeem_cnt");
        Set<String> zhao_cai_bao_redeem_cnt_keys = zhao_cai_bao_redeem_cnt.keySet();
        Object[] zhao_cai_bao_redeem_cnt_keysArr = zhao_cai_bao_redeem_cnt_keys.toArray();
        Arrays.sort(zhao_cai_bao_redeem_cnt_keysArr);
        String zhao_cai_bao_redeem_cnt_1 = "";
        String zhao_cai_bao_redeem_cnt_2 = "";
        String zhao_cai_bao_redeem_cnt_3 = "";
        String zhao_cai_bao_redeem_cnt_4 = "";
        String zhao_cai_bao_redeem_cnt_5 = "";
        String zhao_cai_bao_redeem_cnt_6 = "";
        for (int i = 0; i < zhao_cai_bao_redeem_cnt_keysArr.length; i++ ){
            if (i == 0) zhao_cai_bao_redeem_cnt_1 = zhao_cai_bao_redeem_cnt.getString(String.valueOf(zhao_cai_bao_redeem_cnt_keysArr[0]));
            if (i == 1) zhao_cai_bao_redeem_cnt_2 = zhao_cai_bao_redeem_cnt.getString(String.valueOf(zhao_cai_bao_redeem_cnt_keysArr[1]));
            if (i == 2) zhao_cai_bao_redeem_cnt_3 = zhao_cai_bao_redeem_cnt.getString(String.valueOf(zhao_cai_bao_redeem_cnt_keysArr[2]));
            if (i == 3) zhao_cai_bao_redeem_cnt_4 = zhao_cai_bao_redeem_cnt.getString(String.valueOf(zhao_cai_bao_redeem_cnt_keysArr[3]));
            if (i == 4) zhao_cai_bao_redeem_cnt_5 = zhao_cai_bao_redeem_cnt.getString(String.valueOf(zhao_cai_bao_redeem_cnt_keysArr[4]));
            if (i == 5) zhao_cai_bao_redeem_cnt_6 = zhao_cai_bao_redeem_cnt.getString(String.valueOf(zhao_cai_bao_redeem_cnt_keysArr[5]));
        }
        String zhao_cai_bao_redeem_cnt_sum = zhao_cai_bao_redeem_cnt.getString("sum");

        JSONObject fund_redeem_amt = (JSONObject) other_in_com.get("fund_redeem_amt");
        Set<String> fund_redeem_amt_keys = fund_redeem_amt.keySet();
        Object[] fund_redeem_amt_keysArr = fund_redeem_amt_keys.toArray();
        Arrays.sort(fund_redeem_amt_keysArr);
        String fund_redeem_amt_1 = "";
        String fund_redeem_amt_2 = "";
        String fund_redeem_amt_3 = "";
        String fund_redeem_amt_4 = "";
        String fund_redeem_amt_5 = "";
        String fund_redeem_amt_6 = "";
        for (int i = 0; i < fund_redeem_amt_keysArr.length; i++ ){
            if (i == 0) fund_redeem_amt_1 = fund_redeem_amt.getString(String.valueOf(fund_redeem_amt_keysArr[0]));
            if (i == 1) fund_redeem_amt_2 = fund_redeem_amt.getString(String.valueOf(fund_redeem_amt_keysArr[1]));
            if (i == 2) fund_redeem_amt_3 = fund_redeem_amt.getString(String.valueOf(fund_redeem_amt_keysArr[2]));
            if (i == 3) fund_redeem_amt_4 = fund_redeem_amt.getString(String.valueOf(fund_redeem_amt_keysArr[3]));
            if (i == 4) fund_redeem_amt_5 = fund_redeem_amt.getString(String.valueOf(fund_redeem_amt_keysArr[4]));
            if (i == 5) fund_redeem_amt_6 = fund_redeem_amt.getString(String.valueOf(fund_redeem_amt_keysArr[5]));
        }
        String fund_redeem_amt_sum = fund_redeem_amt.getString("sum");

        JSONObject fund_redeem_cnt = (JSONObject) other_in_com.get("fund_redeem_cnt");
        Set<String> fund_redeem_cnt_keys = fund_redeem_cnt.keySet();
        Object[] fund_redeem_cnt_keysArr = fund_redeem_cnt_keys.toArray();
        Arrays.sort(fund_redeem_cnt_keysArr);
        String fund_redeem_cnt_1 = "";
        String fund_redeem_cnt_2 = "";
        String fund_redeem_cnt_3 = "";
        String fund_redeem_cnt_4 = "";
        String fund_redeem_cnt_5 = "";
        String fund_redeem_cnt_6 = "";
        for (int i = 0; i < fund_redeem_cnt_keysArr.length; i++ ){
            if (i == 0) fund_redeem_cnt_1 = fund_redeem_cnt.getString(String.valueOf(fund_redeem_cnt_keysArr[0]));
            if (i == 1) fund_redeem_cnt_2 = fund_redeem_cnt.getString(String.valueOf(fund_redeem_cnt_keysArr[1]));
            if (i == 2) fund_redeem_cnt_3 = fund_redeem_cnt.getString(String.valueOf(fund_redeem_cnt_keysArr[2]));
            if (i == 3) fund_redeem_cnt_4 = fund_redeem_cnt.getString(String.valueOf(fund_redeem_cnt_keysArr[3]));
            if (i == 4) fund_redeem_cnt_5 = fund_redeem_cnt.getString(String.valueOf(fund_redeem_cnt_keysArr[4]));
            if (i == 5) fund_redeem_cnt_6 = fund_redeem_cnt.getString(String.valueOf(fund_redeem_cnt_keysArr[5]));
        }
        String fund_redeem_cnt_sum = fund_redeem_cnt.getString("sum");

        JSONObject cun_jin_bao_redeem_amt = (JSONObject) other_in_com.get("cun_jin_bao_redeem_amt");
        Set<String> cun_jin_bao_redeem_amt_keys = cun_jin_bao_redeem_amt.keySet();
        Object[] cun_jin_bao_redeem_amt_keysArr = cun_jin_bao_redeem_amt_keys.toArray();
        Arrays.sort(cun_jin_bao_redeem_amt_keysArr);
        String cun_jin_bao_redeem_amt_1 = "";
        String cun_jin_bao_redeem_amt_2 = "";
        String cun_jin_bao_redeem_amt_3 = "";
        String cun_jin_bao_redeem_amt_4 = "";
        String cun_jin_bao_redeem_amt_5 = "";
        String cun_jin_bao_redeem_amt_6 = "";
        for (int i = 0; i < cun_jin_bao_redeem_amt_keysArr.length; i++ ){
            if (i == 0) cun_jin_bao_redeem_amt_1 = cun_jin_bao_redeem_amt.getString(String.valueOf(cun_jin_bao_redeem_amt_keysArr[0]));
            if (i == 1) cun_jin_bao_redeem_amt_2 = cun_jin_bao_redeem_amt.getString(String.valueOf(cun_jin_bao_redeem_amt_keysArr[1]));
            if (i == 2) cun_jin_bao_redeem_amt_3 = cun_jin_bao_redeem_amt.getString(String.valueOf(cun_jin_bao_redeem_amt_keysArr[2]));
            if (i == 3) cun_jin_bao_redeem_amt_4 = cun_jin_bao_redeem_amt.getString(String.valueOf(cun_jin_bao_redeem_amt_keysArr[3]));
            if (i == 4) cun_jin_bao_redeem_amt_5 = cun_jin_bao_redeem_amt.getString(String.valueOf(cun_jin_bao_redeem_amt_keysArr[4]));
            if (i == 5) cun_jin_bao_redeem_amt_6 = cun_jin_bao_redeem_amt.getString(String.valueOf(cun_jin_bao_redeem_amt_keysArr[5]));
        }
        String cun_jin_bao_redeem_amt_sum = cun_jin_bao_redeem_amt.getString("sum");

        JSONObject cun_jin_bao_redeem_cnt = (JSONObject) other_in_com.get("cun_jin_bao_redeem_cnt");
        Set<String> cun_jin_bao_redeem_cnt_keys = cun_jin_bao_redeem_cnt.keySet();
        Object[] cun_jin_bao_redeem_cnt_keysArr = cun_jin_bao_redeem_cnt_keys.toArray();
        Arrays.sort(cun_jin_bao_redeem_cnt_keysArr);
        String cun_jin_bao_redeem_cnt_1 = "";
        String cun_jin_bao_redeem_cnt_2 = "";
        String cun_jin_bao_redeem_cnt_3 = "";
        String cun_jin_bao_redeem_cnt_4 = "";
        String cun_jin_bao_redeem_cnt_5 = "";
        String cun_jin_bao_redeem_cnt_6 = "";
        for (int i = 0; i < cun_jin_bao_redeem_cnt_keysArr.length; i++ ){
            if (i == 0) cun_jin_bao_redeem_cnt_1 = cun_jin_bao_redeem_cnt.getString(String.valueOf(cun_jin_bao_redeem_cnt_keysArr[0]));
            if (i == 1) cun_jin_bao_redeem_cnt_2 = cun_jin_bao_redeem_cnt.getString(String.valueOf(cun_jin_bao_redeem_cnt_keysArr[1]));
            if (i == 2) cun_jin_bao_redeem_cnt_3 = cun_jin_bao_redeem_cnt.getString(String.valueOf(cun_jin_bao_redeem_cnt_keysArr[2]));
            if (i == 3) cun_jin_bao_redeem_cnt_4 = cun_jin_bao_redeem_cnt.getString(String.valueOf(cun_jin_bao_redeem_cnt_keysArr[3]));
            if (i == 4) cun_jin_bao_redeem_cnt_5 = cun_jin_bao_redeem_cnt.getString(String.valueOf(cun_jin_bao_redeem_cnt_keysArr[4]));
            if (i == 5) cun_jin_bao_redeem_cnt_6 = cun_jin_bao_redeem_cnt.getString(String.valueOf(cun_jin_bao_redeem_cnt_keysArr[5]));
        }
        String cun_jin_bao_redeem_cnt_sum = cun_jin_bao_redeem_cnt.getString("sum");

        try {
            stmt.setString(329, jiebei_loan_amt_1 != null ? jiebei_loan_amt_1 : "");
            stmt.setString(330, jiebei_loan_amt_2 != null ? jiebei_loan_amt_2 : "");
            stmt.setString(331, jiebei_loan_amt_3 != null ? jiebei_loan_amt_3 : "");
            stmt.setString(332, jiebei_loan_amt_4 != null ? jiebei_loan_amt_4 : "");
            stmt.setString(333, jiebei_loan_amt_5 != null ? jiebei_loan_amt_5 : "");
            stmt.setString(334, jiebei_loan_amt_6 != null ? jiebei_loan_amt_6 : "");
            stmt.setString(335, jiebei_loan_amt_sum != null ? jiebei_loan_amt_sum : "");
            stmt.setString(336, jiebei_loan_cnt_1 != null ? jiebei_loan_cnt_1 : "");
            stmt.setString(337, jiebei_loan_cnt_2 != null ? jiebei_loan_cnt_2 : "");
            stmt.setString(338, jiebei_loan_cnt_3 != null ? jiebei_loan_cnt_3 : "");
            stmt.setString(339, jiebei_loan_cnt_4 != null ? jiebei_loan_cnt_4 : "");
            stmt.setString(340, jiebei_loan_cnt_5 != null ? jiebei_loan_cnt_5 : "");
            stmt.setString(341, jiebei_loan_cnt_6 != null ? jiebei_loan_cnt_6 : "");
            stmt.setString(342, jiebei_loan_cnt_sum != null ? jiebei_loan_cnt_sum : "");
            stmt.setString(343, other_loan_amt_1 != null ? other_loan_amt_1 : "");
            stmt.setString(344, other_loan_amt_2 != null ? other_loan_amt_2 : "");
            stmt.setString(345, other_loan_amt_3 != null ? other_loan_amt_3 : "");
            stmt.setString(346, other_loan_amt_4 != null ? other_loan_amt_4 : "");
            stmt.setString(347, other_loan_amt_5 != null ? other_loan_amt_5 : "");
            stmt.setString(348, other_loan_amt_6 != null ? other_loan_amt_6 : "");
            stmt.setString(349, other_loan_amt_sum != null ? other_loan_amt_sum : "");
            stmt.setString(350, other_loan_cnt_1 != null ? other_loan_cnt_1 : "");
            stmt.setString(351, other_loan_cnt_2 != null ? other_loan_cnt_2 : "");
            stmt.setString(352, other_loan_cnt_3 != null ? other_loan_cnt_3 : "");
            stmt.setString(353, other_loan_cnt_4 != null ? other_loan_cnt_4 : "");
            stmt.setString(354, other_loan_cnt_5 != null ? other_loan_cnt_5 : "");
            stmt.setString(355, other_loan_cnt_6 != null ? other_loan_cnt_6 : "");
            stmt.setString(356, other_loan_cnt_sum != null ? other_loan_cnt_sum : "");
            stmt.setString(357, transfer_in_amt_1 != null ? transfer_in_amt_1 : "");
            stmt.setString(358, transfer_in_amt_2 != null ? transfer_in_amt_2 : "");
            stmt.setString(359, transfer_in_amt_3 != null ? transfer_in_amt_3 : "");
            stmt.setString(360, transfer_in_amt_4 != null ? transfer_in_amt_4 : "");
            stmt.setString(361, transfer_in_amt_5 != null ? transfer_in_amt_5 : "");
            stmt.setString(362, transfer_in_amt_6 != null ? transfer_in_amt_6 : "");
            stmt.setString(363, transfer_in_amt_sum != null ? transfer_in_amt_sum : "");
            stmt.setString(364, transfer_in_cnt_1 != null ? transfer_in_cnt_1 : "");
            stmt.setString(365, transfer_in_cnt_2 != null ? transfer_in_cnt_2 : "");
            stmt.setString(366, transfer_in_cnt_3 != null ? transfer_in_cnt_3 : "");
            stmt.setString(367, transfer_in_cnt_4 != null ? transfer_in_cnt_4 : "");
            stmt.setString(368, transfer_in_cnt_5 != null ? transfer_in_cnt_5 : "");
            stmt.setString(369, transfer_in_cnt_6 != null ? transfer_in_cnt_6 : "");
            stmt.setString(370, transfer_in_cnt_sum != null ? transfer_in_cnt_sum : "");
            stmt.setString(371, max_transfer_in_amt_1 != null ? max_transfer_in_amt_1 : "");
            stmt.setString(372, max_transfer_in_amt_2 != null ? max_transfer_in_amt_2 : "");
            stmt.setString(373, max_transfer_in_amt_3 != null ? max_transfer_in_amt_3 : "");
            stmt.setString(374, max_transfer_in_amt_4 != null ? max_transfer_in_amt_4 : "");
            stmt.setString(375, max_transfer_in_amt_5 != null ? max_transfer_in_amt_5 : "");
            stmt.setString(376, max_transfer_in_amt_6 != null ? max_transfer_in_amt_6 : "");
            stmt.setString(377, max_transfer_in_amt_sum != null ? max_transfer_in_amt_sum : "");
            stmt.setString(378, refund_amt_1 != null ? refund_amt_1 : "");
            stmt.setString(379, refund_amt_2 != null ? refund_amt_2 : "");
            stmt.setString(380, refund_amt_3 != null ? refund_amt_3 : "");
            stmt.setString(381, refund_amt_4 != null ? refund_amt_4 : "");
            stmt.setString(382, refund_amt_5 != null ? refund_amt_5 : "");
            stmt.setString(383, refund_amt_6 != null ? refund_amt_6 : "");
            stmt.setString(384, refund_amt_sum != null ? refund_amt_sum : "");
            stmt.setString(385, refund_cnt_1 != null ? refund_cnt_1 : "");
            stmt.setString(386, refund_cnt_2 != null ? refund_cnt_2 : "");
            stmt.setString(387, refund_cnt_3 != null ? refund_cnt_3 : "");
            stmt.setString(388, refund_cnt_4 != null ? refund_cnt_4 : "");
            stmt.setString(389, refund_cnt_5 != null ? refund_cnt_5 : "");
            stmt.setString(390, refund_cnt_6 != null ? refund_cnt_6 : "");
            stmt.setString(391, refund_cnt_sum != null ? refund_cnt_sum : "");
            stmt.setString(392, max_refund_amt_1 != null ? max_refund_amt_1 : "");
            stmt.setString(393, max_refund_amt_2 != null ? max_refund_amt_2 : "");
            stmt.setString(394, max_refund_amt_3 != null ? max_refund_amt_3 : "");
            stmt.setString(395, max_refund_amt_4 != null ? max_refund_amt_4 : "");
            stmt.setString(396, max_refund_amt_5 != null ? max_refund_amt_5 : "");
            stmt.setString(397, max_refund_amt_6 != null ? max_refund_amt_6 : "");
            stmt.setString(398, max_refund_amt_sum != null ? max_refund_amt_sum : "");
            stmt.setString(399, yu_e_bao_profit_amt_1 != null ? yu_e_bao_profit_amt_1 : "");
            stmt.setString(400, yu_e_bao_profit_amt_2 != null ? yu_e_bao_profit_amt_2 : "");
            stmt.setString(401, yu_e_bao_profit_amt_3 != null ? yu_e_bao_profit_amt_3 : "");
            stmt.setString(402, yu_e_bao_profit_amt_4 != null ? yu_e_bao_profit_amt_4 : "");
            stmt.setString(403, yu_e_bao_profit_amt_5 != null ? yu_e_bao_profit_amt_5 : "");
            stmt.setString(404, yu_e_bao_profit_amt_6 != null ? yu_e_bao_profit_amt_6 : "");
            stmt.setString(405, yu_e_bao_profit_amt_sum != null ? yu_e_bao_profit_amt_sum : "");
            stmt.setString(406, zhao_cai_bao_redeem_amt_1 != null ? zhao_cai_bao_redeem_amt_1 : "");
            stmt.setString(407, zhao_cai_bao_redeem_amt_2 != null ? zhao_cai_bao_redeem_amt_2 : "");
            stmt.setString(408, zhao_cai_bao_redeem_amt_3 != null ? zhao_cai_bao_redeem_amt_3 : "");
            stmt.setString(409, zhao_cai_bao_redeem_amt_4 != null ? zhao_cai_bao_redeem_amt_4 : "");
            stmt.setString(410, zhao_cai_bao_redeem_amt_5 != null ? zhao_cai_bao_redeem_amt_5 : "");
            stmt.setString(411, zhao_cai_bao_redeem_amt_6 != null ? zhao_cai_bao_redeem_amt_6 : "");
            stmt.setString(412, zhao_cai_bao_redeem_amt_sum != null ? zhao_cai_bao_redeem_amt_sum : "");
            stmt.setString(413, zhao_cai_bao_redeem_cnt_1 != null ? zhao_cai_bao_redeem_cnt_1 : "");
            stmt.setString(414, zhao_cai_bao_redeem_cnt_2 != null ? zhao_cai_bao_redeem_cnt_2 : "");
            stmt.setString(415, zhao_cai_bao_redeem_cnt_3 != null ? zhao_cai_bao_redeem_cnt_3 : "");
            stmt.setString(416, zhao_cai_bao_redeem_cnt_4 != null ? zhao_cai_bao_redeem_cnt_4 : "");
            stmt.setString(417, zhao_cai_bao_redeem_cnt_5 != null ? zhao_cai_bao_redeem_cnt_5 : "");
            stmt.setString(418, zhao_cai_bao_redeem_cnt_6 != null ? zhao_cai_bao_redeem_cnt_6 : "");
            stmt.setString(419, zhao_cai_bao_redeem_cnt_sum != null ? zhao_cai_bao_redeem_cnt_sum : "");
            stmt.setString(420, fund_redeem_amt_1 != null ? fund_redeem_amt_1 : "");
            stmt.setString(421, fund_redeem_amt_2 != null ? fund_redeem_amt_2 : "");
            stmt.setString(422, fund_redeem_amt_3 != null ? fund_redeem_amt_3 : "");
            stmt.setString(423, fund_redeem_amt_4 != null ? fund_redeem_amt_4 : "");
            stmt.setString(424, fund_redeem_amt_5 != null ? fund_redeem_amt_5 : "");
            stmt.setString(425, fund_redeem_amt_6 != null ? fund_redeem_amt_6 : "");
            stmt.setString(426, fund_redeem_amt_sum != null ? fund_redeem_amt_sum : "");
            stmt.setString(427, fund_redeem_cnt_1 != null ? fund_redeem_cnt_1 : "");
            stmt.setString(428, fund_redeem_cnt_2 != null ? fund_redeem_cnt_2 : "");
            stmt.setString(429, fund_redeem_cnt_3 != null ? fund_redeem_cnt_3 : "");
            stmt.setString(430, fund_redeem_cnt_4 != null ? fund_redeem_cnt_4 : "");
            stmt.setString(431, fund_redeem_cnt_5 != null ? fund_redeem_cnt_5 : "");
            stmt.setString(432, fund_redeem_cnt_6 != null ? fund_redeem_cnt_6 : "");
            stmt.setString(433, fund_redeem_cnt_sum != null ? fund_redeem_cnt_sum : "");
            stmt.setString(434, cun_jin_bao_redeem_amt_1 != null ? cun_jin_bao_redeem_amt_1 : "");
            stmt.setString(435, cun_jin_bao_redeem_amt_2 != null ? cun_jin_bao_redeem_amt_2 : "");
            stmt.setString(436, cun_jin_bao_redeem_amt_3 != null ? cun_jin_bao_redeem_amt_3 : "");
            stmt.setString(437, cun_jin_bao_redeem_amt_4 != null ? cun_jin_bao_redeem_amt_4 : "");
            stmt.setString(438, cun_jin_bao_redeem_amt_5 != null ? cun_jin_bao_redeem_amt_5 : "");
            stmt.setString(439, cun_jin_bao_redeem_amt_6 != null ? cun_jin_bao_redeem_amt_6 : "");
            stmt.setString(440, cun_jin_bao_redeem_amt_sum != null ? cun_jin_bao_redeem_amt_sum : "");
            stmt.setString(441, cun_jin_bao_redeem_cnt_1 != null ? cun_jin_bao_redeem_cnt_1 : "");
            stmt.setString(442, cun_jin_bao_redeem_cnt_2 != null ? cun_jin_bao_redeem_cnt_2 : "");
            stmt.setString(443, cun_jin_bao_redeem_cnt_3 != null ? cun_jin_bao_redeem_cnt_3 : "");
            stmt.setString(444, cun_jin_bao_redeem_cnt_4 != null ? cun_jin_bao_redeem_cnt_4 : "");
            stmt.setString(445, cun_jin_bao_redeem_cnt_5 != null ? cun_jin_bao_redeem_cnt_5 : "");
            stmt.setString(446, cun_jin_bao_redeem_cnt_6 != null ? cun_jin_bao_redeem_cnt_6 : "");
            stmt.setString(447, cun_jin_bao_redeem_cnt_sum != null ? cun_jin_bao_redeem_cnt_sum : "");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void category_one(PreparedStatement stmt, JSONObject major_expenditure) {
        //  1
        JSONObject repayment = (JSONObject) major_expenditure.get("repayment");

        JSONObject credit_rpy_amt = (JSONObject) repayment.get("credit_rpy_amt");
        Set<String> credit_rpy_amt_keys = credit_rpy_amt.keySet();
        Object[] credit_rpy_amt_keysArr = credit_rpy_amt_keys.toArray();
        Arrays.sort(credit_rpy_amt_keysArr);
        String credit_rpy_amt_1 = "";
        String credit_rpy_amt_2 = "";
        String credit_rpy_amt_3 = "";
        String credit_rpy_amt_4 = "";
        String credit_rpy_amt_5 = "";
        String credit_rpy_amt_6 = "";
        for (int i = credit_rpy_amt_keysArr.length -1; i >= 0; i-- ){
            if (i == 0) credit_rpy_amt_1 = credit_rpy_amt.getString(String.valueOf(credit_rpy_amt_keysArr[0]));
            if (i == 1) credit_rpy_amt_2 = credit_rpy_amt.getString(String.valueOf(credit_rpy_amt_keysArr[1]));
            if (i == 2) credit_rpy_amt_3 = credit_rpy_amt.getString(String.valueOf(credit_rpy_amt_keysArr[2]));
            if (i == 3) credit_rpy_amt_4 = credit_rpy_amt.getString(String.valueOf(credit_rpy_amt_keysArr[3]));
            if (i == 4) credit_rpy_amt_5 = credit_rpy_amt.getString(String.valueOf(credit_rpy_amt_keysArr[4]));
            if (i == 5) credit_rpy_amt_6 = credit_rpy_amt.getString(String.valueOf(credit_rpy_amt_keysArr[5]));
        }
        String credit_rpy_amt_sum = credit_rpy_amt.getString("sum");

        JSONObject credit_rpy_cnt = (JSONObject) repayment.get("credit_rpy_cnt");
        Set<String> credit_rpy_cnt_keys = credit_rpy_cnt.keySet();
        Object[] credit_rpy_cnt_keysArr = credit_rpy_cnt_keys.toArray();
        Arrays.sort(credit_rpy_cnt_keysArr);
        String credit_rpy_cnt_1 = "";
        String credit_rpy_cnt_2 = "";
        String credit_rpy_cnt_3 = "";
        String credit_rpy_cnt_4 = "";
        String credit_rpy_cnt_5 = "";
        String credit_rpy_cnt_6 = "";
        for (int i = 0; i < credit_rpy_cnt_keysArr.length; i++ ){
            if (i == 0) credit_rpy_cnt_1 = credit_rpy_cnt.getString(String.valueOf(credit_rpy_cnt_keysArr[0]));
            if (i == 1) credit_rpy_cnt_2 = credit_rpy_cnt.getString(String.valueOf(credit_rpy_cnt_keysArr[1]));
            if (i == 2) credit_rpy_cnt_3 = credit_rpy_cnt.getString(String.valueOf(credit_rpy_cnt_keysArr[2]));
            if (i == 3) credit_rpy_cnt_4 = credit_rpy_cnt.getString(String.valueOf(credit_rpy_cnt_keysArr[3]));
            if (i == 4) credit_rpy_cnt_5 = credit_rpy_cnt.getString(String.valueOf(credit_rpy_cnt_keysArr[4]));
            if (i == 5) credit_rpy_cnt_6 = credit_rpy_cnt.getString(String.valueOf(credit_rpy_cnt_keysArr[5]));
        }
        String credit_rpy_cnt_sum = credit_rpy_cnt.getString("sum");

        JSONObject huabei_rpy_amt = (JSONObject) repayment.get("huabei_rpy_amt");
        Set<String> huabei_rpy_amt_keys = huabei_rpy_amt.keySet();
        Object[] huabei_rpy_amt_keysArr = huabei_rpy_amt_keys.toArray();
        Arrays.sort(huabei_rpy_amt_keysArr);
        String huabei_rpy_amt_1 = "";
        String huabei_rpy_amt_2 = "";
        String huabei_rpy_amt_3 = "";
        String huabei_rpy_amt_4 = "";
        String huabei_rpy_amt_5 = "";
        String huabei_rpy_amt_6 = "";
        for (int i = 0; i < huabei_rpy_amt_keysArr.length; i++ ){
            if (i == 0) huabei_rpy_amt_1 = huabei_rpy_amt.getString(String.valueOf(huabei_rpy_amt_keysArr[0]));
            if (i == 1) huabei_rpy_amt_2 = huabei_rpy_amt.getString(String.valueOf(huabei_rpy_amt_keysArr[1]));
            if (i == 2) huabei_rpy_amt_3 = huabei_rpy_amt.getString(String.valueOf(huabei_rpy_amt_keysArr[2]));
            if (i == 3) huabei_rpy_amt_4 = huabei_rpy_amt.getString(String.valueOf(huabei_rpy_amt_keysArr[3]));
            if (i == 4) huabei_rpy_amt_5 = huabei_rpy_amt.getString(String.valueOf(huabei_rpy_amt_keysArr[4]));
            if (i == 5) huabei_rpy_amt_6 = huabei_rpy_amt.getString(String.valueOf(huabei_rpy_amt_keysArr[5]));
        }
        String huabei_rpy_amt_sum = huabei_rpy_amt.getString("sum");

        JSONObject huabei_rpy_cnt = (JSONObject) repayment.get("huabei_rpy_cnt");
        Set<String> huabei_rpy_cnt_keys = huabei_rpy_cnt.keySet();
        Object[] huabei_rpy_cnt_keysArr = huabei_rpy_cnt_keys.toArray();
        Arrays.sort(huabei_rpy_cnt_keysArr);
        String huabei_rpy_cnt_1 = "";
        String huabei_rpy_cnt_2 = "";
        String huabei_rpy_cnt_3 = "";
        String huabei_rpy_cnt_4 = "";
        String huabei_rpy_cnt_5 = "";
        String huabei_rpy_cnt_6 = "";
        for (int i = 0; i < huabei_rpy_cnt_keysArr.length; i++ ){
            if (i == 0) huabei_rpy_cnt_1 = huabei_rpy_cnt.getString(String.valueOf(huabei_rpy_cnt_keysArr[0]));
            if (i == 1) huabei_rpy_cnt_2 = huabei_rpy_cnt.getString(String.valueOf(huabei_rpy_cnt_keysArr[1]));
            if (i == 2) huabei_rpy_cnt_3 = huabei_rpy_cnt.getString(String.valueOf(huabei_rpy_cnt_keysArr[2]));
            if (i == 3) huabei_rpy_cnt_4 = huabei_rpy_cnt.getString(String.valueOf(huabei_rpy_cnt_keysArr[3]));
            if (i == 4) huabei_rpy_cnt_5 = huabei_rpy_cnt.getString(String.valueOf(huabei_rpy_cnt_keysArr[4]));
            if (i == 5) huabei_rpy_cnt_6 = huabei_rpy_cnt.getString(String.valueOf(huabei_rpy_cnt_keysArr[5]));
        }
        String huabei_rpy_cnt_sum = huabei_rpy_cnt.getString("sum");

        JSONObject jiebei_rpy_amt = (JSONObject) repayment.get("jiebei_rpy_amt");
        Set<String> jiebei_rpy_amt_keys = jiebei_rpy_amt.keySet();
        Object[] jiebei_rpy_amt_keysArr = jiebei_rpy_amt_keys.toArray();
        Arrays.sort(jiebei_rpy_amt_keysArr);
        String jiebei_rpy_amt_1 = "";
        String jiebei_rpy_amt_2 = "";
        String jiebei_rpy_amt_3 = "";
        String jiebei_rpy_amt_4 = "";
        String jiebei_rpy_amt_5 = "";
        String jiebei_rpy_amt_6 = "";
        for (int i = 0; i < huabei_rpy_cnt_keysArr.length; i++ ){
            if (i == 0) jiebei_rpy_amt_1 = jiebei_rpy_amt.getString(String.valueOf(jiebei_rpy_amt_keysArr[0]));
            if (i == 1) jiebei_rpy_amt_2 = jiebei_rpy_amt.getString(String.valueOf(jiebei_rpy_amt_keysArr[1]));
            if (i == 2) jiebei_rpy_amt_3 = jiebei_rpy_amt.getString(String.valueOf(jiebei_rpy_amt_keysArr[2]));
            if (i == 3) jiebei_rpy_amt_4 = jiebei_rpy_amt.getString(String.valueOf(jiebei_rpy_amt_keysArr[3]));
            if (i == 4) jiebei_rpy_amt_5 = jiebei_rpy_amt.getString(String.valueOf(jiebei_rpy_amt_keysArr[4]));
            if (i == 5) jiebei_rpy_amt_6 = jiebei_rpy_amt.getString(String.valueOf(jiebei_rpy_amt_keysArr[5]));
        }
        String jiebei_rpy_amt_sum = jiebei_rpy_amt.getString("sum");

        JSONObject jiebei_rpy_cnt = (JSONObject) repayment.get("jiebei_rpy_cnt");
        Set<String> jiebei_rpy_cnt_keys = jiebei_rpy_cnt.keySet();
        Object[] jiebei_rpy_cnt_keysArr = jiebei_rpy_cnt_keys.toArray();
        Arrays.sort(jiebei_rpy_cnt_keysArr);
        String jiebei_rpy_cnt_1 = "";
        String jiebei_rpy_cnt_2 = "";
        String jiebei_rpy_cnt_3 = "";
        String jiebei_rpy_cnt_4 = "";
        String jiebei_rpy_cnt_5 = "";
        String jiebei_rpy_cnt_6 = "";
        for (int i = 0; i < huabei_rpy_cnt_keysArr.length; i++ ){
            if (i == 0) jiebei_rpy_cnt_1 = jiebei_rpy_cnt.getString(String.valueOf(jiebei_rpy_cnt_keysArr[0]));
            if (i == 1) jiebei_rpy_cnt_2 = jiebei_rpy_cnt.getString(String.valueOf(jiebei_rpy_cnt_keysArr[1]));
            if (i == 2) jiebei_rpy_cnt_3 = jiebei_rpy_cnt.getString(String.valueOf(jiebei_rpy_cnt_keysArr[2]));
            if (i == 3) jiebei_rpy_cnt_4 = jiebei_rpy_cnt.getString(String.valueOf(jiebei_rpy_cnt_keysArr[3]));
            if (i == 4) jiebei_rpy_cnt_5 = jiebei_rpy_cnt.getString(String.valueOf(jiebei_rpy_cnt_keysArr[4]));
            if (i == 5) jiebei_rpy_cnt_6 = jiebei_rpy_cnt.getString(String.valueOf(jiebei_rpy_cnt_keysArr[5]));
        }
        String jiebei_rpy_cnt_sum = jiebei_rpy_cnt.getString("sum");

        JSONObject other_rpy_amt = (JSONObject) repayment.get("other_rpy_amt");
        Set<String> other_rpy_amt_keys = other_rpy_amt.keySet();
        Object[] other_rpy_amt_keysArr = other_rpy_amt_keys.toArray();
        Arrays.sort(other_rpy_amt_keysArr);
        String other_rpy_amt_1 = "";
        String other_rpy_amt_2 = "";
        String other_rpy_amt_3 = "";
        String other_rpy_amt_4 = "";
        String other_rpy_amt_5 = "";
        String other_rpy_amt_6 = "";
        for (int i = 0; i < huabei_rpy_cnt_keysArr.length; i++ ){
            if (i == 0) other_rpy_amt_1 = other_rpy_amt.getString(String.valueOf(other_rpy_amt_keysArr[0]));
            if (i == 1) other_rpy_amt_2 = other_rpy_amt.getString(String.valueOf(other_rpy_amt_keysArr[1]));
            if (i == 2) other_rpy_amt_3 = other_rpy_amt.getString(String.valueOf(other_rpy_amt_keysArr[2]));
            if (i == 3) other_rpy_amt_4 = other_rpy_amt.getString(String.valueOf(other_rpy_amt_keysArr[3]));
            if (i == 4) other_rpy_amt_5 = other_rpy_amt.getString(String.valueOf(other_rpy_amt_keysArr[4]));
            if (i == 5) other_rpy_amt_6 = other_rpy_amt.getString(String.valueOf(other_rpy_amt_keysArr[5]));
        }
        String other_rpy_amt_sum = other_rpy_amt.getString("sum");

        JSONObject other_rpy_cnt = (JSONObject) repayment.get("other_rpy_cnt");
        Set<String> other_rpy_cnt_keys = other_rpy_cnt.keySet();
        Object[] other_rpy_cnt_keysArr = other_rpy_cnt_keys.toArray();
        Arrays.sort(other_rpy_cnt_keysArr);
        String other_rpy_cnt_1 = "";
        String other_rpy_cnt_2 = "";
        String other_rpy_cnt_3 = "";
        String other_rpy_cnt_4 = "";
        String other_rpy_cnt_5 = "";
        String other_rpy_cnt_6 = "";
        for (int i = 0; i < other_rpy_cnt_keysArr.length; i++ ){
            if (i == 0) other_rpy_cnt_1 = other_rpy_cnt.getString(String.valueOf(other_rpy_cnt_keysArr[0]));
            if (i == 1) other_rpy_cnt_2 = other_rpy_cnt.getString(String.valueOf(other_rpy_cnt_keysArr[1]));
            if (i == 2) other_rpy_cnt_3 = other_rpy_cnt.getString(String.valueOf(other_rpy_cnt_keysArr[2]));
            if (i == 3) other_rpy_cnt_4 = other_rpy_cnt.getString(String.valueOf(other_rpy_cnt_keysArr[3]));
            if (i == 4) other_rpy_cnt_5 = other_rpy_cnt.getString(String.valueOf(other_rpy_cnt_keysArr[4]));
            if (i == 5) other_rpy_cnt_6 = other_rpy_cnt.getString(String.valueOf(other_rpy_cnt_keysArr[5]));
        }
        String other_rpy_cnt_sum = other_rpy_cnt.getString("sum");

        //  2
        JSONObject consumption = (JSONObject) major_expenditure.get("consumption");

        JSONObject total_consume_amt = (JSONObject) consumption.get("total_consume_amt");
        Set<String> total_consume_amt_keys = total_consume_amt.keySet();
        Object[] total_consume_amt_keysArr = total_consume_amt_keys.toArray();
        Arrays.sort(total_consume_amt_keysArr);
        String total_consume_amt_1 = "";
        String total_consume_amt_2 = "";
        String total_consume_amt_3 = "";
        String total_consume_amt_4 = "";
        String total_consume_amt_5 = "";
        String total_consume_amt_6 = "";
        for (int i = 0; i < total_consume_amt_keysArr.length; i++ ){
            if (i == 0) total_consume_amt_1 = total_consume_amt.getString(String.valueOf(total_consume_amt_keysArr[0]));
            if (i == 1) total_consume_amt_2 = total_consume_amt.getString(String.valueOf(total_consume_amt_keysArr[1]));
            if (i == 2) total_consume_amt_3 = total_consume_amt.getString(String.valueOf(total_consume_amt_keysArr[2]));
            if (i == 3) total_consume_amt_4 = total_consume_amt.getString(String.valueOf(total_consume_amt_keysArr[3]));
            if (i == 4) total_consume_amt_5 = total_consume_amt.getString(String.valueOf(total_consume_amt_keysArr[4]));
            if (i == 5) total_consume_amt_6 = total_consume_amt.getString(String.valueOf(total_consume_amt_keysArr[5]));
        }
        String total_consume_amt_sum = total_consume_amt.getString("sum");

        JSONObject total_consume_cnt = (JSONObject) consumption.get("total_consume_cnt");
        Set<String> total_consume_cnt_keys = total_consume_cnt.keySet();
        Object[] total_consume_cnt_keysArr = total_consume_cnt_keys.toArray();
        Arrays.sort(total_consume_cnt_keysArr);
        String total_consume_cnt_1 = "";
        String total_consume_cnt_2 = "";
        String total_consume_cnt_3 = "";
        String total_consume_cnt_4 = "";
        String total_consume_cnt_5 = "";
        String total_consume_cnt_6 = "";
        for (int i = 0; i < total_consume_cnt_keysArr.length; i++ ){
            if (i == 0) total_consume_cnt_1 = total_consume_cnt.getString(String.valueOf(total_consume_cnt_keysArr[0]));
            if (i == 1) total_consume_cnt_2 = total_consume_cnt.getString(String.valueOf(total_consume_cnt_keysArr[1]));
            if (i == 2) total_consume_cnt_3 = total_consume_cnt.getString(String.valueOf(total_consume_cnt_keysArr[2]));
            if (i == 3) total_consume_cnt_4 = total_consume_cnt.getString(String.valueOf(total_consume_cnt_keysArr[3]));
            if (i == 4) total_consume_cnt_5 = total_consume_cnt.getString(String.valueOf(total_consume_cnt_keysArr[4]));
            if (i == 5) total_consume_cnt_6 = total_consume_cnt.getString(String.valueOf(total_consume_cnt_keysArr[5]));
        }
        String total_consume_cnt_sum = total_consume_cnt.getString("sum");

        JSONObject max_consume_amt = (JSONObject) consumption.get("max_consume_amt");
        Set<String> max_consume_amt_keys = max_consume_amt.keySet();
        Object[] max_consume_amt_keysArr = max_consume_amt_keys.toArray();
        Arrays.sort(max_consume_amt_keysArr);
        String max_consume_amt_1 = "";
        String max_consume_amt_2 = "";
        String max_consume_amt_3 = "";
        String max_consume_amt_4 = "";
        String max_consume_amt_5 = "";
        String max_consume_amt_6 = "";
        for (int i = 0; i < max_consume_amt_keysArr.length; i++ ){
            if (i == 0) max_consume_amt_1 = max_consume_amt.getString(String.valueOf(max_consume_amt_keysArr[0]));
            if (i == 1) max_consume_amt_2 = max_consume_amt.getString(String.valueOf(max_consume_amt_keysArr[1]));
            if (i == 2) max_consume_amt_3 = max_consume_amt.getString(String.valueOf(max_consume_amt_keysArr[2]));
            if (i == 3) max_consume_amt_4 = max_consume_amt.getString(String.valueOf(max_consume_amt_keysArr[3]));
            if (i == 4) max_consume_amt_5 = max_consume_amt.getString(String.valueOf(max_consume_amt_keysArr[4]));
            if (i == 5) max_consume_amt_6 = max_consume_amt.getString(String.valueOf(max_consume_amt_keysArr[5]));
        }
        String max_consume_amt_sum = max_consume_amt.getString("sum");

        JSONObject online_shopping_amt = (JSONObject) consumption.get("online_shopping_amt");
        Set<String> online_shopping_amt_keys = online_shopping_amt.keySet();
        Object[] online_shopping_amt_keysArr = online_shopping_amt_keys.toArray();
        Arrays.sort(online_shopping_amt_keysArr);
        String online_shopping_amt_1 = "";
        String online_shopping_amt_2 = "";
        String online_shopping_amt_3 = "";
        String online_shopping_amt_4 = "";
        String online_shopping_amt_5 = "";
        String online_shopping_amt_6 = "";
        for (int i = 0; i < online_shopping_amt_keysArr.length; i++ ){
            if (i == 0) online_shopping_amt_1 = online_shopping_amt.getString(String.valueOf(online_shopping_amt_keysArr[0]));
            if (i == 1) online_shopping_amt_2 = online_shopping_amt.getString(String.valueOf(online_shopping_amt_keysArr[1]));
            if (i == 2) online_shopping_amt_3 = online_shopping_amt.getString(String.valueOf(online_shopping_amt_keysArr[2]));
            if (i == 3) online_shopping_amt_4 = online_shopping_amt.getString(String.valueOf(online_shopping_amt_keysArr[3]));
            if (i == 4) online_shopping_amt_5 = online_shopping_amt.getString(String.valueOf(online_shopping_amt_keysArr[4]));
            if (i == 5) online_shopping_amt_6 = online_shopping_amt.getString(String.valueOf(online_shopping_amt_keysArr[5]));
        }
        String online_shopping_amt_sum = online_shopping_amt.getString("sum");

        JSONObject online_shopping_cnt = (JSONObject) consumption.get("online_shopping_cnt");
        Set<String> online_shopping_cnt_keys = online_shopping_cnt.keySet();
        Object[] online_shopping_cnt_keysArr = online_shopping_cnt_keys.toArray();
        Arrays.sort(online_shopping_cnt_keysArr);
        String online_shopping_cnt_1 = "";
        String online_shopping_cnt_2 = "";
        String online_shopping_cnt_3 = "";
        String online_shopping_cnt_4 = "";
        String online_shopping_cnt_5 = "";
        String online_shopping_cnt_6 = "";
        for (int i = 0; i < online_shopping_cnt_keysArr.length; i++ ){
            if (i == 0) online_shopping_cnt_1 = online_shopping_cnt.getString(String.valueOf(online_shopping_cnt_keysArr[0]));
            if (i == 1) online_shopping_cnt_2 = online_shopping_cnt.getString(String.valueOf(online_shopping_cnt_keysArr[1]));
            if (i == 2) online_shopping_cnt_3 = online_shopping_cnt.getString(String.valueOf(online_shopping_cnt_keysArr[2]));
            if (i == 3) online_shopping_cnt_4 = online_shopping_cnt.getString(String.valueOf(online_shopping_cnt_keysArr[3]));
            if (i == 4) online_shopping_cnt_5 = online_shopping_cnt.getString(String.valueOf(online_shopping_cnt_keysArr[4]));
            if (i == 5) online_shopping_cnt_6 = online_shopping_cnt.getString(String.valueOf(online_shopping_cnt_keysArr[5]));
        }
        String online_shopping_cnt_sum = online_shopping_cnt.getString("sum");

        JSONObject takeout_amt = (JSONObject) consumption.get("takeout_amt");
        Set<String> takeout_amt_keys = takeout_amt.keySet();
        Object[] takeout_amt_keysArr = takeout_amt_keys.toArray();
        Arrays.sort(takeout_amt_keysArr);
        String takeout_amt_1 = "";
        String takeout_amt_2 = "";
        String takeout_amt_3 = "";
        String takeout_amt_4 = "";
        String takeout_amt_5 = "";
        String takeout_amt_6 = "";
        for (int i = 0; i < takeout_amt_keysArr.length; i++ ){
            if (i == 0) takeout_amt_1 = takeout_amt.getString(String.valueOf(takeout_amt_keysArr[0]));
            if (i == 1) takeout_amt_2 = takeout_amt.getString(String.valueOf(takeout_amt_keysArr[1]));
            if (i == 2) takeout_amt_3 = takeout_amt.getString(String.valueOf(takeout_amt_keysArr[2]));
            if (i == 3) takeout_amt_4 = takeout_amt.getString(String.valueOf(takeout_amt_keysArr[3]));
            if (i == 4) takeout_amt_5 = takeout_amt.getString(String.valueOf(takeout_amt_keysArr[4]));
            if (i == 5) takeout_amt_6 = takeout_amt.getString(String.valueOf(takeout_amt_keysArr[5]));
        }
        String takeout_amt_sum = takeout_amt.getString("sum");

        JSONObject takeout_cnt = (JSONObject) consumption.get("takeout_cnt");
        Set<String> takeout_cnt_keys = takeout_cnt.keySet();
        Object[] takeout_cnt_keysArr = takeout_cnt_keys.toArray();
        Arrays.sort(takeout_cnt_keysArr);
        String takeout_cnt_1 = "";
        String takeout_cnt_2 = "";
        String takeout_cnt_3 = "";
        String takeout_cnt_4 = "";
        String takeout_cnt_5 = "";
        String takeout_cnt_6 = "";
        for (int i = 0; i < takeout_cnt_keysArr.length; i++ ){
            if (i == 0) takeout_cnt_1 = takeout_cnt.getString(String.valueOf(takeout_cnt_keysArr[0]));
            if (i == 1) takeout_cnt_2 = takeout_cnt.getString(String.valueOf(takeout_cnt_keysArr[1]));
            if (i == 2) takeout_cnt_3 = takeout_cnt.getString(String.valueOf(takeout_cnt_keysArr[2]));
            if (i == 3) takeout_cnt_4 = takeout_cnt.getString(String.valueOf(takeout_cnt_keysArr[3]));
            if (i == 4) takeout_cnt_5 = takeout_cnt.getString(String.valueOf(takeout_cnt_keysArr[4]));
            if (i == 5) takeout_cnt_6 = takeout_cnt.getString(String.valueOf(takeout_cnt_keysArr[5]));
        }
        String takeout_cnt_sum = takeout_cnt.getString("sum");

        JSONObject lifepay_amt = (JSONObject) consumption.get("lifepay_amt");
        Set<String> lifepay_amt_keys = lifepay_amt.keySet();
        Object[] lifepay_amt_keysArr = lifepay_amt_keys.toArray();
        Arrays.sort(lifepay_amt_keysArr);
        String lifepay_amt_1 = "";
        String lifepay_amt_2 = "";
        String lifepay_amt_3 = "";
        String lifepay_amt_4 = "";
        String lifepay_amt_5 = "";
        String lifepay_amt_6 = "";
        for (int i = 0; i < lifepay_amt_keysArr.length; i++ ){
            if (i == 0) lifepay_amt_1 = lifepay_amt.getString(String.valueOf(lifepay_amt_keysArr[0]));
            if (i == 1) lifepay_amt_2 = lifepay_amt.getString(String.valueOf(lifepay_amt_keysArr[1]));
            if (i == 2) lifepay_amt_3 = lifepay_amt.getString(String.valueOf(lifepay_amt_keysArr[2]));
            if (i == 3) lifepay_amt_4 = lifepay_amt.getString(String.valueOf(lifepay_amt_keysArr[3]));
            if (i == 4) lifepay_amt_5 = lifepay_amt.getString(String.valueOf(lifepay_amt_keysArr[4]));
            if (i == 5) lifepay_amt_6 = lifepay_amt.getString(String.valueOf(lifepay_amt_keysArr[5]));
        }
        String lifepay_amt_sum = lifepay_amt.getString("sum");

        JSONObject lifepay_cnt = (JSONObject) consumption.get("lifepay_cnt");
        Set<String> lifepay_cnt_keys = lifepay_cnt.keySet();
        Object[] lifepay_cnt_keysArr = lifepay_cnt_keys.toArray();
        Arrays.sort(lifepay_cnt_keysArr);
        String lifepay_cnt_1 = "";
        String lifepay_cnt_2 = "";
        String lifepay_cnt_3 = "";
        String lifepay_cnt_4 = "";
        String lifepay_cnt_5 = "";
        String lifepay_cnt_6 = "";
        for (int i = 0; i < lifepay_cnt_keysArr.length; i++ ){
            if (i == 0) lifepay_cnt_1 = lifepay_cnt.getString(String.valueOf(lifepay_cnt_keysArr[0]));
            if (i == 1) lifepay_cnt_2 = lifepay_cnt.getString(String.valueOf(lifepay_cnt_keysArr[1]));
            if (i == 2) lifepay_cnt_3 = lifepay_cnt.getString(String.valueOf(lifepay_cnt_keysArr[2]));
            if (i == 3) lifepay_cnt_4 = lifepay_cnt.getString(String.valueOf(lifepay_cnt_keysArr[3]));
            if (i == 4) lifepay_cnt_5 = lifepay_cnt.getString(String.valueOf(lifepay_cnt_keysArr[4]));
            if (i == 5) lifepay_cnt_6 = lifepay_cnt.getString(String.valueOf(lifepay_cnt_keysArr[5]));
        }
        String lifepay_cnt_sum = lifepay_cnt.getString("sum");

        JSONObject taxipay_amt = (JSONObject) consumption.get("taxipay_amt");
        Set<String> taxipay_amt_keys = taxipay_amt.keySet();
        Object[] taxipay_amt_keysArr = taxipay_amt_keys.toArray();
        Arrays.sort(taxipay_amt_keysArr);
        String taxipay_amt_1 = "";
        String taxipay_amt_2 = "";
        String taxipay_amt_3 = "";
        String taxipay_amt_4 = "";
        String taxipay_amt_5 = "";
        String taxipay_amt_6 = "";
        for (int i = 0; i < taxipay_amt_keysArr.length; i++ ){
            if (i == 0) taxipay_amt_1 = taxipay_amt.getString(String.valueOf(taxipay_amt_keysArr[0]));
            if (i == 1) taxipay_amt_2 = taxipay_amt.getString(String.valueOf(taxipay_amt_keysArr[1]));
            if (i == 2) taxipay_amt_3 = taxipay_amt.getString(String.valueOf(taxipay_amt_keysArr[2]));
            if (i == 3) taxipay_amt_4 = taxipay_amt.getString(String.valueOf(taxipay_amt_keysArr[3]));
            if (i == 4) taxipay_amt_5 = taxipay_amt.getString(String.valueOf(taxipay_amt_keysArr[4]));
            if (i == 5) taxipay_amt_6 = taxipay_amt.getString(String.valueOf(taxipay_amt_keysArr[5]));
        }
        String taxipay_amt_sum = taxipay_amt.getString("sum");

        JSONObject taxipay_cnt = (JSONObject) consumption.get("taxipay_cnt");
        Set<String> taxipay_cnt_keys = taxipay_cnt.keySet();
        Object[] taxipay_cnt_keysArr = taxipay_cnt_keys.toArray();
        Arrays.sort(taxipay_cnt_keysArr);
        String taxipay_cnt_1 = "";
        String taxipay_cnt_2 = "";
        String taxipay_cnt_3 = "";
        String taxipay_cnt_4 = "";
        String taxipay_cnt_5 = "";
        String taxipay_cnt_6 = "";
        for (int i = 0; i < taxipay_cnt_keysArr.length; i++ ){
            if (i == 0) taxipay_cnt_1 = taxipay_cnt.getString(String.valueOf(taxipay_cnt_keysArr[0]));
            if (i == 1) taxipay_cnt_2 = taxipay_cnt.getString(String.valueOf(taxipay_cnt_keysArr[1]));
            if (i == 2) taxipay_cnt_3 = taxipay_cnt.getString(String.valueOf(taxipay_cnt_keysArr[2]));
            if (i == 3) taxipay_cnt_4 = taxipay_cnt.getString(String.valueOf(taxipay_cnt_keysArr[3]));
            if (i == 4) taxipay_cnt_5 = taxipay_cnt.getString(String.valueOf(taxipay_cnt_keysArr[4]));
            if (i == 5) taxipay_cnt_6 = taxipay_cnt.getString(String.valueOf(taxipay_cnt_keysArr[5]));
        }
        String taxipay_cnt_sum = taxipay_cnt.getString("sum");

        JSONObject carpay_amt = (JSONObject) consumption.get("carpay_amt");
        Set<String> carpay_amt_keys = carpay_amt.keySet();
        Object[] carpay_amt_keysArr = carpay_amt_keys.toArray();
        Arrays.sort(carpay_amt_keysArr);
        String carpay_amt_1 = "";
        String carpay_amt_2 = "";
        String carpay_amt_3 = "";
        String carpay_amt_4 = "";
        String carpay_amt_5 = "";
        String carpay_amt_6 = "";
        for (int i = 0; i < carpay_amt_keysArr.length; i++ ){
            if (i == 0) carpay_amt_1 = carpay_amt.getString(String.valueOf(carpay_amt_keysArr[0]));
            if (i == 1) carpay_amt_2 = carpay_amt.getString(String.valueOf(carpay_amt_keysArr[1]));
            if (i == 2) carpay_amt_3 = carpay_amt.getString(String.valueOf(carpay_amt_keysArr[2]));
            if (i == 3) carpay_amt_4 = carpay_amt.getString(String.valueOf(carpay_amt_keysArr[3]));
            if (i == 4) carpay_amt_5 = carpay_amt.getString(String.valueOf(carpay_amt_keysArr[4]));
            if (i == 5) carpay_amt_6 = carpay_amt.getString(String.valueOf(carpay_amt_keysArr[5]));
        }
        String carpay_amt_sum = carpay_amt.getString("sum");

        JSONObject carpay_cnt = (JSONObject) consumption.get("carpay_cnt");
        Set<String> carpay_cnt_keys = carpay_cnt.keySet();
        Object[] carpay_cnt_keysArr = carpay_cnt_keys.toArray();
        Arrays.sort(carpay_cnt_keysArr);
        String carpay_cnt_1 = "";
        String carpay_cnt_2 = "";
        String carpay_cnt_3 = "";
        String carpay_cnt_4 = "";
        String carpay_cnt_5 = "";
        String carpay_cnt_6 = "";
        for (int i = 0; i < carpay_cnt_keysArr.length; i++ ){
            if (i == 0) carpay_cnt_1 = carpay_cnt.getString(String.valueOf(carpay_cnt_keysArr[0]));
            if (i == 1) carpay_cnt_2 = carpay_cnt.getString(String.valueOf(carpay_cnt_keysArr[1]));
            if (i == 2) carpay_cnt_3 = carpay_cnt.getString(String.valueOf(carpay_cnt_keysArr[2]));
            if (i == 3) carpay_cnt_4 = carpay_cnt.getString(String.valueOf(carpay_cnt_keysArr[3]));
            if (i == 4) carpay_cnt_5 = carpay_cnt.getString(String.valueOf(carpay_cnt_keysArr[4]));
            if (i == 5) carpay_cnt_6 = carpay_cnt.getString(String.valueOf(carpay_cnt_keysArr[5]));
        }
        String carpay_cnt_sum = carpay_cnt.getString("sum");

        JSONObject travel_amt = (JSONObject) consumption.get("travel_amt");
        Set<String> travel_amt_keys = travel_amt.keySet();
        Object[] travel_amt_keysArr = travel_amt_keys.toArray();
        Arrays.sort(travel_amt_keysArr);
        String travel_amt_1 = "";
        String travel_amt_2 = "";
        String travel_amt_3 = "";
        String travel_amt_4 = "";
        String travel_amt_5 = "";
        String travel_amt_6 = "";
        for (int i = 0; i < travel_amt_keysArr.length; i++ ){
            if (i == 0) travel_amt_1 = travel_amt.getString(String.valueOf(travel_amt_keysArr[0]));
            if (i == 1) travel_amt_2 = travel_amt.getString(String.valueOf(travel_amt_keysArr[1]));
            if (i == 2) travel_amt_3 = travel_amt.getString(String.valueOf(travel_amt_keysArr[2]));
            if (i == 3) travel_amt_4 = travel_amt.getString(String.valueOf(travel_amt_keysArr[3]));
            if (i == 4) travel_amt_5 = travel_amt.getString(String.valueOf(travel_amt_keysArr[4]));
            if (i == 5) travel_amt_6 = travel_amt.getString(String.valueOf(travel_amt_keysArr[5]));
        }
        String travel_amt_sum = travel_amt.getString("sum");

        JSONObject travel_cnt = (JSONObject) consumption.get("travel_cnt");
        Set<String> travel_cnt_keys = travel_cnt.keySet();
        Object[] travel_cnt_keysArr = travel_cnt_keys.toArray();
        Arrays.sort(travel_cnt_keysArr);
        String travel_cnt_1 = "";
        String travel_cnt_2 = "";
        String travel_cnt_3 = "";
        String travel_cnt_4 = "";
        String travel_cnt_5 = "";
        String travel_cnt_6 = "";
        for (int i = 0; i < travel_cnt_keysArr.length; i++ ){
            if (i == 0) travel_cnt_1 = travel_cnt.getString(String.valueOf(travel_cnt_keysArr[0]));
            if (i == 1) travel_cnt_2 = travel_cnt.getString(String.valueOf(travel_cnt_keysArr[1]));
            if (i == 2) travel_cnt_3 = travel_cnt.getString(String.valueOf(travel_cnt_keysArr[2]));
            if (i == 3) travel_cnt_4 = travel_cnt.getString(String.valueOf(travel_cnt_keysArr[3]));
            if (i == 4) travel_cnt_5 = travel_cnt.getString(String.valueOf(travel_cnt_keysArr[4]));
            if (i == 5) travel_cnt_6 = travel_cnt.getString(String.valueOf(travel_cnt_keysArr[5]));
        }
        String travel_cnt_sum = travel_cnt.getString("sum");

        JSONObject lottery_amt = (JSONObject) consumption.get("lottery_amt");
        Set<String> lottery_amt_keys = lottery_amt.keySet();
        Object[] lottery_amt_keysArr = lottery_amt_keys.toArray();
        Arrays.sort(lottery_amt_keysArr);
        String lottery_amt_1 = "";
        String lottery_amt_2 = "";
        String lottery_amt_3 = "";
        String lottery_amt_4 = "";
        String lottery_amt_5 = "";
        String lottery_amt_6 = "";
        for (int i = 0; i < lottery_amt_keysArr.length; i++ ){
            if (i == 0) lottery_amt_1 = lottery_amt.getString(String.valueOf(lottery_amt_keysArr[0]));
            if (i == 1) lottery_amt_2 = lottery_amt.getString(String.valueOf(lottery_amt_keysArr[1]));
            if (i == 2) lottery_amt_3 = lottery_amt.getString(String.valueOf(lottery_amt_keysArr[2]));
            if (i == 3) lottery_amt_4 = lottery_amt.getString(String.valueOf(lottery_amt_keysArr[3]));
            if (i == 4) lottery_amt_5 = lottery_amt.getString(String.valueOf(lottery_amt_keysArr[4]));
            if (i == 5) lottery_amt_6 = lottery_amt.getString(String.valueOf(lottery_amt_keysArr[5]));
        }
        String lottery_amt_sum = lottery_amt.getString("sum");

        JSONObject lottery_rate = (JSONObject) consumption.get("lottery_rate");
        Set<String> lottery_rate_keys = lottery_rate.keySet();
        Object[] lottery_rate_keysArr = lottery_rate_keys.toArray();
        Arrays.sort(lottery_rate_keysArr);
        String lottery_rate_1 = "";
        String lottery_rate_2 = "";
        String lottery_rate_3 = "";
        String lottery_rate_4 = "";
        String lottery_rate_5 = "";
        String lottery_rate_6 = "";
        for (int i = 0; i < lottery_rate_keysArr.length; i++ ){
            if (i == 0) lottery_rate_1 = lottery_rate.getString(String.valueOf(lottery_rate_keysArr[0]));
            if (i == 1) lottery_rate_2 = lottery_rate.getString(String.valueOf(lottery_rate_keysArr[1]));
            if (i == 2) lottery_rate_3 = lottery_rate.getString(String.valueOf(lottery_rate_keysArr[2]));
            if (i == 3) lottery_rate_4 = lottery_rate.getString(String.valueOf(lottery_rate_keysArr[3]));
            if (i == 4) lottery_rate_5 = lottery_rate.getString(String.valueOf(lottery_rate_keysArr[4]));
            if (i == 5) lottery_rate_6 = lottery_rate.getString(String.valueOf(lottery_rate_keysArr[5]));
        }
        String lottery_rate_sum = lottery_rate.getString("sum");

        JSONObject lottery_cnt = (JSONObject) consumption.get("lottery_cnt");
        Set<String> lottery_cnt_keys = lottery_cnt.keySet();
        Object[] lottery_cnt_keysArr = lottery_cnt_keys.toArray();
        Arrays.sort(lottery_cnt_keysArr);
        String lottery_cnt_1 = "";
        String lottery_cnt_2 = "";
        String lottery_cnt_3 = "";
        String lottery_cnt_4 = "";
        String lottery_cnt_5 = "";
        String lottery_cnt_6 = "";
        for (int i = 0; i < lottery_rate_keysArr.length; i++ ){
            if (i == 0) lottery_cnt_1 = lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[0]));
            if (i == 1) lottery_cnt_2 = lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[1]));
            if (i == 2) lottery_cnt_3 = lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[2]));
            if (i == 3) lottery_cnt_4 = lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[3]));
            if (i == 4) lottery_cnt_5 = lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[4]));
            if (i == 5) lottery_cnt_6 = lottery_cnt.getString(String.valueOf(lottery_cnt_keysArr[5]));
        }
        String lottery_cnt_sum = lottery_cnt.getString("sum");

        JSONObject game_amt = (JSONObject) consumption.get("game_amt");
        Set<String> game_amt_keys = game_amt.keySet();
        Object[] game_amt_keysArr = game_amt_keys.toArray();
        Arrays.sort(game_amt_keysArr);
        String game_amt_1 = "";
        String game_amt_2 = "";
        String game_amt_3 = "";
        String game_amt_4 = "";
        String game_amt_5 = "";
        String game_amt_6 = "";
        for (int i = 0; i < game_amt_keysArr.length; i++ ){
            if (i == 0) game_amt_1 = game_amt.getString(String.valueOf(game_amt_keysArr[0]));
            if (i == 1) game_amt_2 = game_amt.getString(String.valueOf(game_amt_keysArr[1]));
            if (i == 2) game_amt_3 = game_amt.getString(String.valueOf(game_amt_keysArr[2]));
            if (i == 3) game_amt_4 = game_amt.getString(String.valueOf(game_amt_keysArr[3]));
            if (i == 4) game_amt_5 = game_amt.getString(String.valueOf(game_amt_keysArr[4]));
            if (i == 5) game_amt_6 = game_amt.getString(String.valueOf(game_amt_keysArr[5]));
        }
        String game_amt_sum = game_amt.getString("sum");

        JSONObject game_rate = (JSONObject) consumption.get("game_rate");
        Set<String> game_rate_keys = game_rate.keySet();
        Object[] game_rate_keysArr = game_rate_keys.toArray();
        Arrays.sort(game_rate_keysArr);
        String game_rate_1 = "";
        String game_rate_2 = "";
        String game_rate_3 = "";
        String game_rate_4 = "";
        String game_rate_5 = "";
        String game_rate_6 = "";
        for (int i = 0; i < game_rate_keysArr.length; i++ ){
            if (i == 0) game_rate_1 = game_rate.getString(String.valueOf(game_rate_keysArr[0]));
            if (i == 1) game_rate_2 = game_rate.getString(String.valueOf(game_rate_keysArr[1]));
            if (i == 2) game_rate_3 = game_rate.getString(String.valueOf(game_rate_keysArr[2]));
            if (i == 3) game_rate_4 = game_rate.getString(String.valueOf(game_rate_keysArr[3]));
            if (i == 4) game_rate_5 = game_rate.getString(String.valueOf(game_rate_keysArr[4]));
            if (i == 5) game_rate_6 = game_rate.getString(String.valueOf(game_rate_keysArr[5]));
        }
        String game_rate_sum = game_rate.getString("sum");

        JSONObject game_cnt = (JSONObject) consumption.get("game_cnt");
        Set<String> game_cnt_keys = game_cnt.keySet();
        Object[] game_cnt_keysArr = game_cnt_keys.toArray();
        Arrays.sort(game_cnt_keysArr);
        String game_cnt_1 = "";
        String game_cnt_2 = "";
        String game_cnt_3 = "";
        String game_cnt_4 = "";
        String game_cnt_5 = "";
        String game_cnt_6 = "";
        for (int i = 0; i < game_cnt_keysArr.length; i++ ){
            if (i == 0) game_cnt_1 = game_cnt.getString(String.valueOf(game_cnt_keysArr[0]));
            if (i == 1) game_cnt_2 = game_cnt.getString(String.valueOf(game_cnt_keysArr[1]));
            if (i == 2) game_cnt_3 = game_cnt.getString(String.valueOf(game_cnt_keysArr[2]));
            if (i == 3) game_cnt_4 = game_cnt.getString(String.valueOf(game_cnt_keysArr[3]));
            if (i == 4) game_cnt_5 = game_cnt.getString(String.valueOf(game_cnt_keysArr[4]));
            if (i == 5) game_cnt_6 = game_cnt.getString(String.valueOf(game_cnt_keysArr[5]));
        }
        String game_cnt_sum = game_cnt.getString("sum");

        //  3
        JSONObject transfer_out_info = (JSONObject) major_expenditure.get("transfer_out_info");
        JSONObject out_amt = (JSONObject) transfer_out_info.get("out_amt");
        Set<String> out_amt_keys = out_amt.keySet();
        Object[] out_amt_keysArr = out_amt_keys.toArray();
        Arrays.sort(out_amt_keysArr);
        String out_amt_1 = "";
        String out_amt_2 = "";
        String out_amt_3 = "";
        String out_amt_4 = "";
        String out_amt_5 = "";
        String out_amt_6 = "";
        for (int i = 0; i < out_amt_keysArr.length; i++ ){
            if (i == 0) out_amt_1 = out_amt.getString(String.valueOf(out_amt_keysArr[0]));
            if (i == 1) out_amt_2 = out_amt.getString(String.valueOf(out_amt_keysArr[1]));
            if (i == 2) out_amt_3 = out_amt.getString(String.valueOf(out_amt_keysArr[2]));
            if (i == 3) out_amt_4 = out_amt.getString(String.valueOf(out_amt_keysArr[3]));
            if (i == 4) out_amt_5 = out_amt.getString(String.valueOf(out_amt_keysArr[4]));
            if (i == 5) out_amt_6 = out_amt.getString(String.valueOf(out_amt_keysArr[5]));
        }
        String out_amt_sum = out_amt.getString("sum");

        JSONObject out_cnt = (JSONObject) transfer_out_info.get("out_cnt");
        Set<String> out_cnt_keys = out_cnt.keySet();
        Object[] out_cnt_keysArr = out_cnt_keys.toArray();
        Arrays.sort(out_cnt_keysArr);
        String out_cnt_1 = "";
        String out_cnt_2 = "";
        String out_cnt_3 = "";
        String out_cnt_4 = "";
        String out_cnt_5 = "";
        String out_cnt_6 = "";
        for (int i = 0; i < out_cnt_keysArr.length; i++ ){
            if (i == 0) out_cnt_1 = out_cnt.getString(String.valueOf(out_cnt_keysArr[0]));
            if (i == 1) out_cnt_2 = out_cnt.getString(String.valueOf(out_cnt_keysArr[1]));
            if (i == 2) out_cnt_3 = out_cnt.getString(String.valueOf(out_cnt_keysArr[2]));
            if (i == 3) out_cnt_4 = out_cnt.getString(String.valueOf(out_cnt_keysArr[3]));
            if (i == 4) out_cnt_5 = out_cnt.getString(String.valueOf(out_cnt_keysArr[4]));
            if (i == 5) out_cnt_6 = out_cnt.getString(String.valueOf(out_cnt_keysArr[5]));
        }
        String out_cnt_sum = out_cnt.getString("sum");

        JSONObject max_out_amt = (JSONObject) transfer_out_info.get("max_out_amt");
        Set<String> max_out_amt_keys = max_out_amt.keySet();
        Object[] max_out_amt_keysArr = max_out_amt_keys.toArray();
        Arrays.sort(max_out_amt_keysArr);
        String max_out_amt_1 = "";
        String max_out_amt_2 = "";
        String max_out_amt_3 = "";
        String max_out_amt_4 = "";
        String max_out_amt_5 = "";
        String max_out_amt_6 = "";
        for (int i = 0; i < max_out_amt_keysArr.length; i++ ){
            if (i == 0) max_out_amt_1 = max_out_amt.getString(String.valueOf(max_out_amt_keysArr[0]));
            if (i == 1) max_out_amt_2 = max_out_amt.getString(String.valueOf(max_out_amt_keysArr[1]));
            if (i == 2) max_out_amt_3 = max_out_amt.getString(String.valueOf(max_out_amt_keysArr[2]));
            if (i == 3) max_out_amt_4 = max_out_amt.getString(String.valueOf(max_out_amt_keysArr[3]));
            if (i == 4) max_out_amt_5 = max_out_amt.getString(String.valueOf(max_out_amt_keysArr[4]));
            if (i == 5) max_out_amt_6 = max_out_amt.getString(String.valueOf(max_out_amt_keysArr[5]));
        }
        String max_out_amt_sum = max_out_amt.getString("sum");

        //  4
        JSONObject financial = (JSONObject) major_expenditure.get("financial");
        JSONObject zhao_cai_bao_purchase_amt = (JSONObject) financial.get("zhao_cai_bao_purchase_amt");
        Set<String> zhao_cai_bao_purchase_amt_keys = zhao_cai_bao_purchase_amt.keySet();
        Object[] zhao_cai_bao_purchase_amt_keysArr = zhao_cai_bao_purchase_amt_keys.toArray();
        Arrays.sort(zhao_cai_bao_purchase_amt_keysArr);
        String zhao_cai_bao_purchase_amt_1 = "";
        String zhao_cai_bao_purchase_amt_2 = "";
        String zhao_cai_bao_purchase_amt_3 = "";
        String zhao_cai_bao_purchase_amt_4 = "";
        String zhao_cai_bao_purchase_amt_5 = "";
        String zhao_cai_bao_purchase_amt_6 = "";
        for (int i = 0; i < zhao_cai_bao_purchase_amt_keysArr.length; i++ ){
            if (i == 0) zhao_cai_bao_purchase_amt_1 = zhao_cai_bao_purchase_amt.getString(String.valueOf (zhao_cai_bao_purchase_amt_keysArr[0]));
            if (i == 1) zhao_cai_bao_purchase_amt_2 = zhao_cai_bao_purchase_amt.getString(String.valueOf(zhao_cai_bao_purchase_amt_keysArr[1]));
            if (i == 2) zhao_cai_bao_purchase_amt_3 = zhao_cai_bao_purchase_amt.getString(String.valueOf(zhao_cai_bao_purchase_amt_keysArr[2]));
            if (i == 3) zhao_cai_bao_purchase_amt_4 = zhao_cai_bao_purchase_amt.getString(String.valueOf(zhao_cai_bao_purchase_amt_keysArr[3]));
            if (i == 4) zhao_cai_bao_purchase_amt_5 = zhao_cai_bao_purchase_amt.getString(String.valueOf(zhao_cai_bao_purchase_amt_keysArr[4]));
            if (i == 5) zhao_cai_bao_purchase_amt_6 = zhao_cai_bao_purchase_amt.getString(String.valueOf(zhao_cai_bao_purchase_amt_keysArr[5]));
        }
        String zhao_cai_bao_purchase_amt_sum = zhao_cai_bao_purchase_amt.getString("sum");

        JSONObject fund_purchase_amt = (JSONObject) financial.get("fund_purchase_amt");
        Set<String> fund_purchase_amt_keys = fund_purchase_amt.keySet();
        Object[] fund_purchase_amt_keysArr = fund_purchase_amt_keys.toArray();
        Arrays.sort(fund_purchase_amt_keysArr);
        String fund_purchase_amt_1 = "";
        String fund_purchase_amt_2 = "";
        String fund_purchase_amt_3 = "";
        String fund_purchase_amt_4 = "";
        String fund_purchase_amt_5 = "";
        String fund_purchase_amt_6 = "";
        for (int i = 0; i < fund_purchase_amt_keysArr.length; i++ ){
            if (i == 0) fund_purchase_amt_1 = fund_purchase_amt.getString(String.valueOf(fund_purchase_amt_keysArr[0]));
            if (i == 1) fund_purchase_amt_2 = fund_purchase_amt.getString(String.valueOf(fund_purchase_amt_keysArr[1]));
            if (i == 2) fund_purchase_amt_3 = fund_purchase_amt.getString(String.valueOf(fund_purchase_amt_keysArr[2]));
            if (i == 3) fund_purchase_amt_4 = fund_purchase_amt.getString(String.valueOf(fund_purchase_amt_keysArr[3]));
            if (i == 4) fund_purchase_amt_5 = fund_purchase_amt.getString(String.valueOf(fund_purchase_amt_keysArr[4]));
            if (i == 5) fund_purchase_amt_6 = fund_purchase_amt.getString(String.valueOf(fund_purchase_amt_keysArr[5]));
        }
        String fund_purchase_amt_sum = fund_purchase_amt.getString("sum");

        JSONObject cun_jin_bao_purchase_amt = (JSONObject) financial.get("cun_jin_bao_purchase_amt");
        Set<String> cun_jin_bao_purchase_amt_keys = cun_jin_bao_purchase_amt.keySet();
        Object[] cun_jin_bao_purchase_amt_keysArr = cun_jin_bao_purchase_amt_keys.toArray();
        Arrays.sort(cun_jin_bao_purchase_amt_keysArr);
        String cun_jin_bao_purchase_amt_1 = "";
        String cun_jin_bao_purchase_amt_2 = "";
        String cun_jin_bao_purchase_amt_3 = "";
        String cun_jin_bao_purchase_amt_4 = "";
        String cun_jin_bao_purchase_amt_5 = "";
        String cun_jin_bao_purchase_amt_6 = "";
        for (int i = 0; i < cun_jin_bao_purchase_amt_keysArr.length; i++ ){
            if (i == 0) cun_jin_bao_purchase_amt_1 = cun_jin_bao_purchase_amt.getString(String.valueOf(cun_jin_bao_purchase_amt_keysArr[0]));
            if (i == 1) cun_jin_bao_purchase_amt_2 = cun_jin_bao_purchase_amt.getString(String.valueOf(cun_jin_bao_purchase_amt_keysArr[1]));
            if (i == 2) cun_jin_bao_purchase_amt_3 = cun_jin_bao_purchase_amt.getString(String.valueOf(cun_jin_bao_purchase_amt_keysArr[2]));
            if (i == 3) cun_jin_bao_purchase_amt_4 = cun_jin_bao_purchase_amt.getString(String.valueOf(cun_jin_bao_purchase_amt_keysArr[3]));
            if (i == 4) cun_jin_bao_purchase_amt_5 = cun_jin_bao_purchase_amt.getString(String.valueOf(cun_jin_bao_purchase_amt_keysArr[4]));
            if (i == 5) cun_jin_bao_purchase_amt_6 = cun_jin_bao_purchase_amt.getString(String.valueOf(cun_jin_bao_purchase_amt_keysArr[5]));
        }
        String cun_jin_bao_purchase_amt_sum = cun_jin_bao_purchase_amt.getString("sum");

        //  5
        JSONObject other_expense = (JSONObject) major_expenditure.get("other_expense");
        JSONObject redpkt_amt = (JSONObject) other_expense.get("redpkt_amt");
        Set<String> redpkt_amt_keys = redpkt_amt.keySet();
        Object[] redpkt_amt_keysArr = redpkt_amt_keys.toArray();
        Arrays.sort(redpkt_amt_keysArr);
        String redpkt_amt_1 = "";
        String redpkt_amt_2 = "";
        String redpkt_amt_3 = "";
        String redpkt_amt_4 = "";
        String redpkt_amt_5 = "";
        String redpkt_amt_6 = "";
        for (int i = 0; i < redpkt_amt_keysArr.length; i++ ){
            if (i == 0) redpkt_amt_1 = redpkt_amt.getString(String.valueOf(redpkt_amt_keysArr[0]));
            if (i == 1) redpkt_amt_2 = redpkt_amt.getString(String.valueOf(redpkt_amt_keysArr[1]));
            if (i == 2) redpkt_amt_3 = redpkt_amt.getString(String.valueOf(redpkt_amt_keysArr[2]));
            if (i == 3) redpkt_amt_4 = redpkt_amt.getString(String.valueOf(redpkt_amt_keysArr[3]));
            if (i == 4) redpkt_amt_5 = redpkt_amt.getString(String.valueOf(redpkt_amt_keysArr[4]));
            if (i == 5) redpkt_amt_6 = redpkt_amt.getString(String.valueOf(redpkt_amt_keysArr[5]));
        }
        String redpkt_amt_sum = redpkt_amt.getString("sum");

        JSONObject redpkt_cnt = (JSONObject) other_expense.get("redpkt_cnt");
        Set<String> redpkt_cnt_keys = redpkt_cnt.keySet();
        Object[] redpkt_cnt_keysArr = redpkt_cnt_keys.toArray();
        Arrays.sort(redpkt_cnt_keysArr);
        String redpkt_cnt_1 = "";
        String redpkt_cnt_2 = "";
        String redpkt_cnt_3 = "";
        String redpkt_cnt_4 = "";
        String redpkt_cnt_5 = "";
        String redpkt_cnt_6 = "";
        for (int i = 0; i < redpkt_cnt_keysArr.length; i++ ){
            if (i == 0) redpkt_cnt_1 = redpkt_cnt.getString(String.valueOf(redpkt_cnt_keysArr[0]));
            if (i == 1) redpkt_cnt_2 = redpkt_cnt.getString(String.valueOf(redpkt_cnt_keysArr[1]));
            if (i == 2) redpkt_cnt_3 = redpkt_cnt.getString(String.valueOf(redpkt_cnt_keysArr[2]));
            if (i == 3) redpkt_cnt_4 = redpkt_cnt.getString(String.valueOf(redpkt_cnt_keysArr[3]));
            if (i == 4) redpkt_cnt_5 = redpkt_cnt.getString(String.valueOf(redpkt_cnt_keysArr[4]));
            if (i == 5) redpkt_cnt_6 = redpkt_cnt.getString(String.valueOf(redpkt_cnt_keysArr[5]));
        }
        String redpkt_cnt_sum = redpkt_cnt.getString("sum");

        JSONObject max_redpkt_amt = (JSONObject) other_expense.get("max_redpkt_amt");
        Set<String> max_redpkt_amt_keys = max_redpkt_amt.keySet();
        Object[] max_redpkt_amt_keysArr = max_redpkt_amt_keys.toArray();
        Arrays.sort(max_redpkt_amt_keysArr);
        String max_redpkt_amt_1 = "";
        String max_redpkt_amt_2 = "";
        String max_redpkt_amt_3 = "";
        String max_redpkt_amt_4 = "";
        String max_redpkt_amt_5 = "";
        String max_redpkt_amt_6 = "";
        for (int i = 0; i < max_redpkt_amt_keysArr.length; i++ ){
            if (i == 0) max_redpkt_amt_1 = max_redpkt_amt.getString(String.valueOf(max_redpkt_amt_keysArr[0]));
            if (i == 1) max_redpkt_amt_2 = max_redpkt_amt.getString(String.valueOf(max_redpkt_amt_keysArr[1]));
            if (i == 2) max_redpkt_amt_3 = max_redpkt_amt.getString(String.valueOf(max_redpkt_amt_keysArr[2]));
            if (i == 3) max_redpkt_amt_4 = max_redpkt_amt.getString(String.valueOf(max_redpkt_amt_keysArr[3]));
            if (i == 4) max_redpkt_amt_5 = max_redpkt_amt.getString(String.valueOf(max_redpkt_amt_keysArr[4]));
            if (i == 5) max_redpkt_amt_6 = max_redpkt_amt.getString(String.valueOf(max_redpkt_amt_keysArr[5]));
        }
        String max_redpkt_amt_sum = max_redpkt_amt.getString("sum");

        JSONObject donate_amt = (JSONObject) other_expense.get("donate_amt");
        Set<String> donate_amt_keys = donate_amt.keySet();
        Object[] donate_amt_keysArr = donate_amt_keys.toArray();
        Arrays.sort(donate_amt_keysArr);
        String donate_amt_1 = "";
        String donate_amt_2 = "";
        String donate_amt_3 = "";
        String donate_amt_4 = "";
        String donate_amt_5 = "";
        String donate_amt_6 = "";
        for (int i = 0; i < donate_amt_keysArr.length; i++ ){
            if (i == 0) donate_amt_1 = donate_amt.getString(String.valueOf(donate_amt_keysArr[0]));
            if (i == 1) donate_amt_2 = donate_amt.getString(String.valueOf(donate_amt_keysArr[1]));
            if (i == 2) donate_amt_3 = donate_amt.getString(String.valueOf(donate_amt_keysArr[2]));
            if (i == 3) donate_amt_4 = donate_amt.getString(String.valueOf(donate_amt_keysArr[3]));
            if (i == 4) donate_amt_5 = donate_amt.getString(String.valueOf(donate_amt_keysArr[4]));
            if (i == 5) donate_amt_6 = donate_amt.getString(String.valueOf(donate_amt_keysArr[5]));
        }
        String donate_amt_sum = donate_amt.getString("sum");

        JSONObject donate_cnt = (JSONObject) other_expense.get("donate_cnt");
        Set<String> donate_cnt_keys = donate_cnt.keySet();
        Object[] donate_cnt_keysArr = donate_cnt_keys.toArray();
        Arrays.sort(donate_cnt_keysArr);
        String donate_cnt_1 = "";
        String donate_cnt_2 = "";
        String donate_cnt_3 = "";
        String donate_cnt_4 = "";
        String donate_cnt_5 = "";
        String donate_cnt_6 = "";
        for (int i = 0; i < donate_cnt_keysArr.length; i++ ){
            if (i == 0) donate_cnt_1 = donate_cnt.getString(String.valueOf(donate_cnt_keysArr[0]));
            if (i == 1) donate_cnt_2 = donate_cnt.getString(String.valueOf(donate_cnt_keysArr[1]));
            if (i == 2) donate_cnt_3 = donate_cnt.getString(String.valueOf(donate_cnt_keysArr[2]));
            if (i == 3) donate_cnt_4 = donate_cnt.getString(String.valueOf(donate_cnt_keysArr[3]));
            if (i == 4) donate_cnt_5 = donate_cnt.getString(String.valueOf(donate_cnt_keysArr[4]));
            if (i == 5) donate_cnt_6 = donate_cnt.getString(String.valueOf(donate_cnt_keysArr[5]));
        }
        String donate_cnt_sum = donate_cnt.getString("sum");

        JSONObject gratuity_amt = (JSONObject) other_expense.get("gratuity_amt");
        Set<String> gratuity_amt_keys = gratuity_amt.keySet();
        Object[] gratuity_amt_keysArr = gratuity_amt_keys.toArray();
        Arrays.sort(gratuity_amt_keysArr);
        String gratuity_amt_1 = "";
        String gratuity_amt_2 = "";
        String gratuity_amt_3 = "";
        String gratuity_amt_4 = "";
        String gratuity_amt_5 = "";
        String gratuity_amt_6 = "";
        for (int i = 0; i < gratuity_amt_keysArr.length; i++ ){
            if (i == 0) gratuity_amt_1 = gratuity_amt.getString(String.valueOf(gratuity_amt_keysArr[0]));
            if (i == 1) gratuity_amt_2 = gratuity_amt.getString(String.valueOf(gratuity_amt_keysArr[1]));
            if (i == 2) gratuity_amt_3 = gratuity_amt.getString(String.valueOf(gratuity_amt_keysArr[2]));
            if (i == 3) gratuity_amt_4 = gratuity_amt.getString(String.valueOf(gratuity_amt_keysArr[3]));
            if (i == 4) gratuity_amt_5 = gratuity_amt.getString(String.valueOf(gratuity_amt_keysArr[4]));
            if (i == 5) gratuity_amt_6 = gratuity_amt.getString(String.valueOf(gratuity_amt_keysArr[5]));
        }
        String gratuity_amt_sum = gratuity_amt.getString("sum");

        JSONObject gratuity_cnt = (JSONObject) other_expense.get("gratuity_cnt");
        Set<String> gratuity_cnt_keys = gratuity_cnt.keySet();
        Object[] gratuity_cnt_keysArr = gratuity_cnt_keys.toArray();
        Arrays.sort(gratuity_cnt_keysArr);
        String gratuity_cnt_1 = "";
        String gratuity_cnt_2 = "";
        String gratuity_cnt_3 = "";
        String gratuity_cnt_4 = "";
        String gratuity_cnt_5 = "";
        String gratuity_cnt_6 = "";
        for (int i = 0; i < gratuity_cnt_keysArr.length; i++ ){
            if (i == 0) gratuity_cnt_1 = gratuity_cnt.getString(String.valueOf(gratuity_cnt_keysArr[0]));
            if (i == 1) gratuity_cnt_2 = gratuity_cnt.getString(String.valueOf(gratuity_cnt_keysArr[1]));
            if (i == 2) gratuity_cnt_3 = gratuity_cnt.getString(String.valueOf(gratuity_cnt_keysArr[2]));
            if (i == 3) gratuity_cnt_4 = gratuity_cnt.getString(String.valueOf(gratuity_cnt_keysArr[3]));
            if (i == 4) gratuity_cnt_5 = gratuity_cnt.getString(String.valueOf(gratuity_cnt_keysArr[4]));
            if (i == 5) gratuity_cnt_6 = gratuity_cnt.getString(String.valueOf(gratuity_cnt_keysArr[5]));
        }
        String gratuity_cnt_sum = gratuity_cnt.getString("sum");

        JSONObject pay_for_ohter_amt = (JSONObject) other_expense.get("pay_for_ohter_amt");
        Set<String> pay_for_ohter_amt_keys = pay_for_ohter_amt.keySet();
        Object[] pay_for_ohter_amt_keysArr = pay_for_ohter_amt_keys.toArray();
        Arrays.sort(pay_for_ohter_amt_keysArr);
        String pay_for_ohter_amt_1 = "";
        String pay_for_ohter_amt_2 = "";
        String pay_for_ohter_amt_3 = "";
        String pay_for_ohter_amt_4 = "";
        String pay_for_ohter_amt_5 = "";
        String pay_for_ohter_amt_6 = "";
        for (int i = 0; i < pay_for_ohter_amt_keysArr.length; i++ ){
            if (i == 0) pay_for_ohter_amt_1 = pay_for_ohter_amt.getString(String.valueOf(pay_for_ohter_amt_keysArr[0]));
            if (i == 1) pay_for_ohter_amt_2 = pay_for_ohter_amt.getString(String.valueOf(pay_for_ohter_amt_keysArr[1]));
            if (i == 2) pay_for_ohter_amt_3 = pay_for_ohter_amt.getString(String.valueOf(pay_for_ohter_amt_keysArr[2]));
            if (i == 3) pay_for_ohter_amt_4 = pay_for_ohter_amt.getString(String.valueOf(pay_for_ohter_amt_keysArr[3]));
            if (i == 4) pay_for_ohter_amt_5 = pay_for_ohter_amt.getString(String.valueOf(pay_for_ohter_amt_keysArr[4]));
            if (i == 5) pay_for_ohter_amt_6 = pay_for_ohter_amt.getString(String.valueOf(pay_for_ohter_amt_keysArr[5]));
        }
        String pay_for_ohter_amt_sum = pay_for_ohter_amt.getString("sum");

        JSONObject pay_for_ohter_cnt = (JSONObject) other_expense.get("pay_for_ohter_cnt");
        Set<String> pay_for_ohter_cnt_keys = pay_for_ohter_cnt.keySet();
        Object[] pay_for_ohter_cnt_keysArr = pay_for_ohter_cnt_keys.toArray();
        Arrays.sort(pay_for_ohter_cnt_keysArr);
        String pay_for_ohter_cnt_1 = "";
        String pay_for_ohter_cnt_2 = "";
        String pay_for_ohter_cnt_3 = "";
        String pay_for_ohter_cnt_4 = "";
        String pay_for_ohter_cnt_5 = "";
        String pay_for_ohter_cnt_6 = "";
        for (int i = 0; i < pay_for_ohter_cnt_keysArr.length; i++ ){
            if (i == 0) pay_for_ohter_cnt_1 = pay_for_ohter_cnt.getString(String.valueOf(pay_for_ohter_cnt_keysArr[0]));
            if (i == 1) pay_for_ohter_cnt_2 = pay_for_ohter_cnt.getString(String.valueOf(pay_for_ohter_cnt_keysArr[1]));
            if (i == 2) pay_for_ohter_cnt_3 = pay_for_ohter_cnt.getString(String.valueOf(pay_for_ohter_cnt_keysArr[2]));
            if (i == 3) pay_for_ohter_cnt_4 = pay_for_ohter_cnt.getString(String.valueOf(pay_for_ohter_cnt_keysArr[3]));
            if (i == 4) pay_for_ohter_cnt_5 = pay_for_ohter_cnt.getString(String.valueOf(pay_for_ohter_cnt_keysArr[4]));
            if (i == 5) pay_for_ohter_cnt_6 = pay_for_ohter_cnt.getString(String.valueOf(pay_for_ohter_cnt_keysArr[5]));
        }
        String pay_for_ohter_cnt_sum = pay_for_ohter_cnt.getString("sum");

        try {
            stmt.setString(21, credit_rpy_amt_1 != null ? credit_rpy_amt_1 : "");
            stmt.setString(22, credit_rpy_amt_2 != null ? credit_rpy_amt_2 : "");
            stmt.setString(23, credit_rpy_amt_3 != null ? credit_rpy_amt_3 : "");
            stmt.setString(24, credit_rpy_amt_4 != null ? credit_rpy_amt_4 : "");
            stmt.setString(25, credit_rpy_amt_5 != null ? credit_rpy_amt_5 : "");
            stmt.setString(26, credit_rpy_amt_6 != null ? credit_rpy_amt_6 : "");
            stmt.setString(27, credit_rpy_amt_sum != null ? credit_rpy_amt_sum : "");
            stmt.setString(28, credit_rpy_cnt_1 != null ? credit_rpy_cnt_1 : "");
            stmt.setString(29, credit_rpy_cnt_2 != null ? credit_rpy_cnt_2 : "");
            stmt.setString(30, credit_rpy_cnt_3 != null ? credit_rpy_cnt_3 : "");
            stmt.setString(31, credit_rpy_cnt_4 != null ? credit_rpy_cnt_4 : "");
            stmt.setString(32, credit_rpy_cnt_5 != null ? credit_rpy_cnt_5 : "");
            stmt.setString(33, credit_rpy_cnt_6 != null ? credit_rpy_cnt_6 : "");
            stmt.setString(34, credit_rpy_cnt_sum != null ? credit_rpy_cnt_sum : "");
            stmt.setString(35, huabei_rpy_amt_1 != null ? huabei_rpy_amt_1 : "");
            stmt.setString(36, huabei_rpy_amt_2 != null ? huabei_rpy_amt_2 : "");
            stmt.setString(37, huabei_rpy_amt_3 != null ? huabei_rpy_amt_3 : "");
            stmt.setString(38, huabei_rpy_amt_4 != null ? huabei_rpy_amt_4 : "");
            stmt.setString(39, huabei_rpy_amt_5 != null ? huabei_rpy_amt_5 : "");
            stmt.setString(40, huabei_rpy_amt_6 != null ? huabei_rpy_amt_6 : "");
            stmt.setString(41, huabei_rpy_amt_sum != null ? huabei_rpy_amt_sum : "");
            stmt.setString(42, huabei_rpy_cnt_1 != null ? huabei_rpy_cnt_1 : "");
            stmt.setString(43, huabei_rpy_cnt_2 != null ? huabei_rpy_cnt_2 : "");
            stmt.setString(44, huabei_rpy_cnt_3 != null ? huabei_rpy_cnt_3 : "");
            stmt.setString(45, huabei_rpy_cnt_4 != null ? huabei_rpy_cnt_4 : "");
            stmt.setString(46, huabei_rpy_cnt_5 != null ? huabei_rpy_cnt_5 : "");
            stmt.setString(47, huabei_rpy_cnt_6 != null ? huabei_rpy_cnt_6 : "");
            stmt.setString(48, huabei_rpy_cnt_sum != null ? huabei_rpy_cnt_sum : "");
            stmt.setString(49, jiebei_rpy_amt_1 != null ? jiebei_rpy_amt_1 : "");
            stmt.setString(50, jiebei_rpy_amt_2 != null ? jiebei_rpy_amt_2 : "");
            stmt.setString(51, jiebei_rpy_amt_3 != null ? jiebei_rpy_amt_3 : "");
            stmt.setString(52, jiebei_rpy_amt_4 != null ? jiebei_rpy_amt_4 : "");
            stmt.setString(53, jiebei_rpy_amt_5 != null ? jiebei_rpy_amt_5 : "");
            stmt.setString(54, jiebei_rpy_amt_6 != null ? jiebei_rpy_amt_6 : "");
            stmt.setString(55, jiebei_rpy_amt_sum != null ? jiebei_rpy_amt_sum : "");
            stmt.setString(56, jiebei_rpy_cnt_1 != null ? jiebei_rpy_cnt_1 : "");
            stmt.setString(57, jiebei_rpy_cnt_2 != null ? jiebei_rpy_cnt_2 : "");
            stmt.setString(58, jiebei_rpy_cnt_3 != null ? jiebei_rpy_cnt_3 : "");
            stmt.setString(59, jiebei_rpy_cnt_4 != null ? jiebei_rpy_cnt_4 : "");
            stmt.setString(60, jiebei_rpy_cnt_5 != null ? jiebei_rpy_cnt_5 : "");
            stmt.setString(61, jiebei_rpy_cnt_6 != null ? jiebei_rpy_cnt_6 : "");
            stmt.setString(62, jiebei_rpy_cnt_sum != null ? jiebei_rpy_cnt_sum : "");
            stmt.setString(63, other_rpy_amt_1 != null ? other_rpy_amt_1 : "");
            stmt.setString(64, other_rpy_amt_2 != null ? other_rpy_amt_2 : "");
            stmt.setString(65, other_rpy_amt_3 != null ? other_rpy_amt_3 : "");
            stmt.setString(66, other_rpy_amt_4 != null ? other_rpy_amt_4 : "");
            stmt.setString(67, other_rpy_amt_5 != null ? other_rpy_amt_5 : "");
            stmt.setString(68, other_rpy_amt_6 != null ? other_rpy_amt_6 : "");
            stmt.setString(69, other_rpy_amt_sum != null ? other_rpy_amt_sum : "");
            stmt.setString(70, other_rpy_cnt_1 != null ? other_rpy_cnt_1 : "");
            stmt.setString(71, other_rpy_cnt_2 != null ? other_rpy_cnt_2 : "");
            stmt.setString(72, other_rpy_cnt_3 != null ? other_rpy_cnt_3 : "");
            stmt.setString(73, other_rpy_cnt_4 != null ? other_rpy_cnt_4 : "");
            stmt.setString(74, other_rpy_cnt_5 != null ? other_rpy_cnt_5 : "");
            stmt.setString(75, other_rpy_cnt_6 != null ? other_rpy_cnt_6 : "");
            stmt.setString(76, other_rpy_cnt_sum != null ? other_rpy_cnt_sum : "");
            stmt.setString(77, total_consume_amt_1 != null ? total_consume_amt_1 : "");
            stmt.setString(78, total_consume_amt_2 != null ? total_consume_amt_2 : "");
            stmt.setString(79, total_consume_amt_3 != null ? total_consume_amt_3 : "");
            stmt.setString(80, total_consume_amt_4 != null ? total_consume_amt_4 : "");
            stmt.setString(81, total_consume_amt_5 != null ? total_consume_amt_5 : "");
            stmt.setString(82, total_consume_amt_6 != null ? total_consume_amt_6 : "");
            stmt.setString(83, total_consume_amt_sum != null ? total_consume_amt_sum : "");
            stmt.setString(84, total_consume_cnt_1 != null ? total_consume_cnt_1 : "");
            stmt.setString(85, total_consume_cnt_2 != null ? total_consume_cnt_2 : "");
            stmt.setString(86, total_consume_cnt_3 != null ? total_consume_cnt_3 : "");
            stmt.setString(87, total_consume_cnt_4 != null ? total_consume_cnt_4 : "");
            stmt.setString(88, total_consume_cnt_5 != null ? total_consume_cnt_5 : "");
            stmt.setString(89, total_consume_cnt_6 != null ? total_consume_cnt_6 : "");
            stmt.setString(90, total_consume_cnt_sum != null ? total_consume_cnt_sum : "");
            stmt.setString(91, max_consume_amt_1 != null ? max_consume_amt_1 : "");
            stmt.setString(92, max_consume_amt_2 != null ? max_consume_amt_2 : "");
            stmt.setString(93, max_consume_amt_3 != null ? max_consume_amt_3 : "");
            stmt.setString(94, max_consume_amt_4 != null ? max_consume_amt_4 : "");
            stmt.setString(95, max_consume_amt_5 != null ? max_consume_amt_5 : "");
            stmt.setString(96, max_consume_amt_6 != null ? max_consume_amt_6 : "");
            stmt.setString(97, max_consume_amt_sum != null ? max_consume_amt_sum : "");
            stmt.setString(98, online_shopping_amt_1 != null ? online_shopping_amt_1 : "");
            stmt.setString(99, online_shopping_amt_2 != null ? online_shopping_amt_2 : "");
            stmt.setString(100, online_shopping_amt_3 != null ? online_shopping_amt_3 : "");
            stmt.setString(101, online_shopping_amt_4 != null ? online_shopping_amt_4 : "");
            stmt.setString(102, online_shopping_amt_5 != null ? online_shopping_amt_5 : "");
            stmt.setString(103, online_shopping_amt_6 != null ? online_shopping_amt_6 : "");
            stmt.setString(104, online_shopping_amt_sum != null ? online_shopping_amt_sum : "");
            stmt.setString(105, online_shopping_cnt_1 != null ? online_shopping_cnt_1 : "");
            stmt.setString(106, online_shopping_cnt_2 != null ? online_shopping_cnt_2 : "");
            stmt.setString(107, online_shopping_cnt_3 != null ? online_shopping_cnt_3 : "");
            stmt.setString(108, online_shopping_cnt_4 != null ? online_shopping_cnt_4 : "");
            stmt.setString(109, online_shopping_cnt_5 != null ? online_shopping_cnt_5 : "");
            stmt.setString(110, online_shopping_cnt_6 != null ? online_shopping_cnt_6 : "");
            stmt.setString(111, online_shopping_cnt_sum != null ? online_shopping_cnt_sum : "");
            stmt.setString(112, takeout_amt_1 != null ? takeout_amt_1 : "");
            stmt.setString(113, takeout_amt_2 != null ? takeout_amt_2 : "");
            stmt.setString(114, takeout_amt_3 != null ? takeout_amt_3 : "");
            stmt.setString(115, takeout_amt_4 != null ? takeout_amt_4 : "");
            stmt.setString(116, takeout_amt_5 != null ? takeout_amt_5 : "");
            stmt.setString(117, takeout_amt_6 != null ? takeout_amt_6 : "");
            stmt.setString(118, takeout_amt_sum != null ? takeout_amt_sum : "");
            stmt.setString(119, takeout_cnt_1 != null ? takeout_cnt_1 : "");
            stmt.setString(120, takeout_cnt_2 != null ? takeout_cnt_2 : "");
            stmt.setString(121, takeout_cnt_3 != null ? takeout_cnt_3 : "");
            stmt.setString(122, takeout_cnt_4 != null ? takeout_cnt_4 : "");
            stmt.setString(123, takeout_cnt_5 != null ? takeout_cnt_5 : "");
            stmt.setString(124, takeout_cnt_6 != null ? takeout_cnt_6 : "");
            stmt.setString(125, takeout_cnt_sum != null ? takeout_cnt_sum : "");
            stmt.setString(126, lifepay_amt_1 != null ? lifepay_amt_1 : "");
            stmt.setString(127, lifepay_amt_2 != null ? lifepay_amt_2 : "");
            stmt.setString(128, lifepay_amt_3 != null ? lifepay_amt_3 : "");
            stmt.setString(129, lifepay_amt_4 != null ? lifepay_amt_4 : "");
            stmt.setString(130, lifepay_amt_5 != null ? lifepay_amt_5 : "");
            stmt.setString(131, lifepay_amt_6 != null ? lifepay_amt_6 : "");
            stmt.setString(132, lifepay_amt_sum != null ? lifepay_amt_sum : "");
            stmt.setString(133, lifepay_cnt_1 != null ? lifepay_cnt_1 : "");
            stmt.setString(134, lifepay_cnt_2 != null ? lifepay_cnt_2 : "");
            stmt.setString(135, lifepay_cnt_3 != null ? lifepay_cnt_3 : "");
            stmt.setString(136, lifepay_cnt_4 != null ? lifepay_cnt_4 : "");
            stmt.setString(137, lifepay_cnt_5 != null ? lifepay_cnt_5 : "");
            stmt.setString(138, lifepay_cnt_6 != null ? lifepay_cnt_6 : "");
            stmt.setString(139, lifepay_cnt_sum != null ? lifepay_cnt_sum : "");
            stmt.setString(140, taxipay_amt_1 != null ? taxipay_amt_1 : "");
            stmt.setString(141, taxipay_amt_2 != null ? taxipay_amt_2 : "");
            stmt.setString(142, taxipay_amt_3 != null ? taxipay_amt_3 : "");
            stmt.setString(143, taxipay_amt_4 != null ? taxipay_amt_4 : "");
            stmt.setString(144, taxipay_amt_5 != null ? taxipay_amt_5 : "");
            stmt.setString(145, taxipay_amt_6 != null ? taxipay_amt_6 : "");
            stmt.setString(146, taxipay_amt_sum != null ? taxipay_amt_sum : "");
            stmt.setString(147, taxipay_cnt_1 != null ? taxipay_cnt_1 : "");
            stmt.setString(148, taxipay_cnt_2 != null ? taxipay_cnt_2 : "");
            stmt.setString(149, taxipay_cnt_3 != null ? taxipay_cnt_3 : "");
            stmt.setString(150, taxipay_cnt_4 != null ? taxipay_cnt_4 : "");
            stmt.setString(151, taxipay_cnt_5 != null ? taxipay_cnt_5 : "");
            stmt.setString(152, taxipay_cnt_6 != null ? taxipay_cnt_6 : "");
            stmt.setString(153, taxipay_cnt_sum != null ? taxipay_cnt_sum : "");
            stmt.setString(154, carpay_amt_1 != null ? carpay_amt_1 : "");
            stmt.setString(155, carpay_amt_2 != null ? carpay_amt_2 : "");
            stmt.setString(156, carpay_amt_3 != null ? carpay_amt_3 : "");
            stmt.setString(157, carpay_amt_4 != null ? carpay_amt_4 : "");
            stmt.setString(158, carpay_amt_5 != null ? carpay_amt_5 : "");
            stmt.setString(159, carpay_amt_6 != null ? carpay_amt_6 : "");
            stmt.setString(160, carpay_amt_sum != null ? carpay_amt_sum : "");
            stmt.setString(161, carpay_cnt_1 != null ? carpay_cnt_1 : "");
            stmt.setString(162, carpay_cnt_2 != null ? carpay_cnt_2 : "");
            stmt.setString(163, carpay_cnt_3 != null ? carpay_cnt_3 : "");
            stmt.setString(164, carpay_cnt_4 != null ? carpay_cnt_4 : "");
            stmt.setString(165, carpay_cnt_5 != null ? carpay_cnt_5 : "");
            stmt.setString(166, carpay_cnt_6 != null ? carpay_cnt_6 : "");
            stmt.setString(167, carpay_cnt_sum != null ? carpay_cnt_sum : "");
            stmt.setString(168, travel_amt_1 != null ? travel_amt_1 : "");
            stmt.setString(169, travel_amt_2 != null ? travel_amt_2 : "");
            stmt.setString(170, travel_amt_3 != null ? travel_amt_3 : "");
            stmt.setString(171, travel_amt_4 != null ? travel_amt_4 : "");
            stmt.setString(172, travel_amt_5 != null ? travel_amt_5 : "");
            stmt.setString(173, travel_amt_6 != null ? travel_amt_6 : "");
            stmt.setString(174, travel_amt_sum != null ? travel_amt_sum : "");
            stmt.setString(175, travel_cnt_1 != null ? travel_cnt_1 : "");
            stmt.setString(176, travel_cnt_2 != null ? travel_cnt_2 : "");
            stmt.setString(177, travel_cnt_3 != null ? travel_cnt_3 : "");
            stmt.setString(178, travel_cnt_4 != null ? travel_cnt_4 : "");
            stmt.setString(179, travel_cnt_5 != null ? travel_cnt_5 : "");
            stmt.setString(180, travel_cnt_6 != null ? travel_cnt_6 : "");
            stmt.setString(181, travel_cnt_sum != null ? travel_cnt_sum : "");
            stmt.setString(182, lottery_amt_1 != null ? lottery_amt_1 : "");
            stmt.setString(183, lottery_amt_2 != null ? lottery_amt_2 : "");
            stmt.setString(184, lottery_amt_3 != null ? lottery_amt_3 : "");
            stmt.setString(185, lottery_amt_4 != null ? lottery_amt_4 : "");
            stmt.setString(186, lottery_amt_5 != null ? lottery_amt_5 : "");
            stmt.setString(187, lottery_amt_6 != null ? lottery_amt_6 : "");
            stmt.setString(188, lottery_amt_sum != null ? lottery_amt_sum : "");
            stmt.setString(189, lottery_rate_1 != null ? lottery_rate_1 : "");
            stmt.setString(190, lottery_rate_2 != null ? lottery_rate_2 : "");
            stmt.setString(191, lottery_rate_3 != null ? lottery_rate_3 : "");
            stmt.setString(192, lottery_rate_4 != null ? lottery_rate_4 : "");
            stmt.setString(193, lottery_rate_5 != null ? lottery_rate_5 : "");
            stmt.setString(194, lottery_rate_6 != null ? lottery_rate_6 : "");
            stmt.setString(195, lottery_rate_sum != null ? lottery_rate_sum : "");
            stmt.setString(196, lottery_cnt_1 != null ? lottery_cnt_1 : "");
            stmt.setString(197, lottery_cnt_2 != null ? lottery_cnt_2 : "");
            stmt.setString(198, lottery_cnt_3 != null ? lottery_cnt_3 : "");
            stmt.setString(199, lottery_cnt_4 != null ? lottery_cnt_4 : "");
            stmt.setString(200, lottery_cnt_5 != null ? lottery_cnt_5 : "");
            stmt.setString(201, lottery_cnt_6 != null ? lottery_cnt_6 : "");
            stmt.setString(202, lottery_cnt_sum != null ? lottery_cnt_sum : "");
            stmt.setString(203, game_amt_1 != null ? game_amt_1 : "");
            stmt.setString(204, game_amt_2 != null ? game_amt_2 : "");
            stmt.setString(205, game_amt_3 != null ? game_amt_3 : "");
            stmt.setString(206, game_amt_4 != null ? game_amt_4 : "");
            stmt.setString(207, game_amt_5 != null ? game_amt_5 : "");
            stmt.setString(208, game_amt_6 != null ? game_amt_6 : "");
            stmt.setString(209, game_amt_sum != null ? game_amt_sum : "");
            stmt.setString(210, game_rate_1 != null ? game_rate_1 : "");
            stmt.setString(211, game_rate_2 != null ? game_rate_2 : "");
            stmt.setString(212, game_rate_3 != null ? game_rate_3 : "");
            stmt.setString(213, game_rate_4 != null ? game_rate_4 : "");
            stmt.setString(214, game_rate_5 != null ? game_rate_5 : "");
            stmt.setString(215, game_rate_6 != null ? game_rate_6 : "");
            stmt.setString(216, game_rate_sum != null ? game_rate_sum : "");
            stmt.setString(217, game_cnt_1 != null ? game_cnt_1 : "");
            stmt.setString(218, game_cnt_2 != null ? game_cnt_2 : "");
            stmt.setString(219, game_cnt_3 != null ? game_cnt_3 : "");
            stmt.setString(220, game_cnt_4 != null ? game_cnt_4 : "");
            stmt.setString(221, game_cnt_5 != null ? game_cnt_5 : "");
            stmt.setString(222, game_cnt_6 != null ? game_cnt_6 : "");
            stmt.setString(223, game_cnt_sum != null ? game_cnt_sum : "");
            stmt.setString(224, out_amt_1 != null ? out_amt_1 : "");
            stmt.setString(225, out_amt_2 != null ? out_amt_2 : "");
            stmt.setString(226, out_amt_3 != null ? out_amt_3 : "");
            stmt.setString(227, out_amt_4 != null ? out_amt_4 : "");
            stmt.setString(228, out_amt_5 != null ? out_amt_5 : "");
            stmt.setString(229, out_amt_6 != null ? out_amt_6 : "");
            stmt.setString(230, out_amt_sum != null ? out_amt_sum : "");
            stmt.setString(231, out_cnt_1 != null ? out_cnt_1 : "");
            stmt.setString(232, out_cnt_2 != null ? out_cnt_2 : "");
            stmt.setString(233, out_cnt_3 != null ? out_cnt_3 : "");
            stmt.setString(234, out_cnt_4 != null ? out_cnt_4 : "");
            stmt.setString(235, out_cnt_5 != null ? out_cnt_5 : "");
            stmt.setString(236, out_cnt_6 != null ? out_cnt_6 : "");
            stmt.setString(237, out_cnt_sum != null ? out_cnt_sum : "");
            stmt.setString(238, max_out_amt_1 != null ? max_out_amt_1 : "");
            stmt.setString(239, max_out_amt_2 != null ? max_out_amt_2 : "");
            stmt.setString(240, max_out_amt_3 != null ? max_out_amt_3 : "");
            stmt.setString(241, max_out_amt_4 != null ? max_out_amt_4 : "");
            stmt.setString(242, max_out_amt_5 != null ? max_out_amt_5 : "");
            stmt.setString(243, max_out_amt_6 != null ? max_out_amt_6 : "");
            stmt.setString(244, max_out_amt_sum != null ? max_out_amt_sum : "");
            stmt.setString(245, zhao_cai_bao_purchase_amt_1 != null ? zhao_cai_bao_purchase_amt_1 : "");
            stmt.setString(246, zhao_cai_bao_purchase_amt_2 != null ? zhao_cai_bao_purchase_amt_2 : "");
            stmt.setString(247, zhao_cai_bao_purchase_amt_3 != null ? zhao_cai_bao_purchase_amt_3 : "");
            stmt.setString(248, zhao_cai_bao_purchase_amt_4 != null ? zhao_cai_bao_purchase_amt_4 : "");
            stmt.setString(249, zhao_cai_bao_purchase_amt_5 != null ? zhao_cai_bao_purchase_amt_5 : "");
            stmt.setString(250, zhao_cai_bao_purchase_amt_6 != null ? zhao_cai_bao_purchase_amt_6 : "");
            stmt.setString(251, zhao_cai_bao_purchase_amt_sum != null ? zhao_cai_bao_purchase_amt_sum : "");
            stmt.setString(252, fund_purchase_amt_1 != null ? fund_purchase_amt_1 : "");
            stmt.setString(253, fund_purchase_amt_2 != null ? fund_purchase_amt_2 : "");
            stmt.setString(254, fund_purchase_amt_3 != null ? fund_purchase_amt_3 : "");
            stmt.setString(255, fund_purchase_amt_4 != null ? fund_purchase_amt_4 : "");
            stmt.setString(256, fund_purchase_amt_5 != null ? fund_purchase_amt_5 : "");
            stmt.setString(257, fund_purchase_amt_6 != null ? fund_purchase_amt_6 : "");
            stmt.setString(258, fund_purchase_amt_sum != null ? fund_purchase_amt_sum : "");
            stmt.setString(259, cun_jin_bao_purchase_amt_1 != null ? cun_jin_bao_purchase_amt_1 : "");
            stmt.setString(260, cun_jin_bao_purchase_amt_2 != null ? cun_jin_bao_purchase_amt_2 : "");
            stmt.setString(261, cun_jin_bao_purchase_amt_3 != null ? cun_jin_bao_purchase_amt_3 : "");
            stmt.setString(262, cun_jin_bao_purchase_amt_4 != null ? cun_jin_bao_purchase_amt_4 : "");
            stmt.setString(263, cun_jin_bao_purchase_amt_5 != null ? cun_jin_bao_purchase_amt_5 : "");
            stmt.setString(264, cun_jin_bao_purchase_amt_6 != null ? cun_jin_bao_purchase_amt_6 : "");
            stmt.setString(265, cun_jin_bao_purchase_amt_sum != null ? cun_jin_bao_purchase_amt_sum : "");
            stmt.setString(266, redpkt_amt_1 != null ? redpkt_amt_1 : "");
            stmt.setString(267, redpkt_amt_2 != null ? redpkt_amt_2 : "");
            stmt.setString(268, redpkt_amt_3 != null ? redpkt_amt_3 : "");
            stmt.setString(269, redpkt_amt_4 != null ? redpkt_amt_4 : "");
            stmt.setString(270, redpkt_amt_5 != null ? redpkt_amt_5 : "");
            stmt.setString(271, redpkt_amt_6 != null ? redpkt_amt_6 : "");
            stmt.setString(272, redpkt_amt_sum != null ? redpkt_amt_sum : "");
            stmt.setString(273, redpkt_cnt_1 != null ? redpkt_cnt_1 : "");
            stmt.setString(274, redpkt_cnt_2 != null ? redpkt_cnt_2 : "");
            stmt.setString(275, redpkt_cnt_3 != null ? redpkt_cnt_3 : "");
            stmt.setString(276, redpkt_cnt_4 != null ? redpkt_cnt_4 : "");
            stmt.setString(277, redpkt_cnt_5 != null ? redpkt_cnt_5 : "");
            stmt.setString(278, redpkt_cnt_6 != null ? redpkt_cnt_6 : "");
            stmt.setString(279, redpkt_cnt_sum != null ? redpkt_cnt_sum : "");
            stmt.setString(280, max_redpkt_amt_1 != null ? max_redpkt_amt_1 : "");
            stmt.setString(281, max_redpkt_amt_2 != null ? max_redpkt_amt_2 : "");
            stmt.setString(282, max_redpkt_amt_3 != null ? max_redpkt_amt_3 : "");
            stmt.setString(283, max_redpkt_amt_4 != null ? max_redpkt_amt_4 : "");
            stmt.setString(284, max_redpkt_amt_5 != null ? max_redpkt_amt_5 : "");
            stmt.setString(285, max_redpkt_amt_6 != null ? max_redpkt_amt_6 : "");
            stmt.setString(286, max_redpkt_amt_sum != null ? max_redpkt_amt_sum : "");
            stmt.setString(287, donate_amt_1 != null ? donate_amt_1 : "");
            stmt.setString(288, donate_amt_2 != null ? donate_amt_2 : "");
            stmt.setString(289, donate_amt_3 != null ? donate_amt_3 : "");
            stmt.setString(290, donate_amt_4 != null ? donate_amt_4 : "");
            stmt.setString(291, donate_amt_5 != null ? donate_amt_5 : "");
            stmt.setString(292, donate_amt_6 != null ? donate_amt_6 : "");
            stmt.setString(293, donate_amt_sum != null ? donate_amt_sum : "");
            stmt.setString(294, donate_cnt_1 != null ? donate_cnt_1 : "");
            stmt.setString(295, donate_cnt_2 != null ? donate_cnt_2 : "");
            stmt.setString(296, donate_cnt_3 != null ? donate_cnt_3 : "");
            stmt.setString(297, donate_cnt_4 != null ? donate_cnt_4 : "");
            stmt.setString(298, donate_cnt_5 != null ? donate_cnt_5 : "");
            stmt.setString(299, donate_cnt_6 != null ? donate_cnt_6 : "");
            stmt.setString(300, donate_cnt_sum != null ? donate_cnt_sum : "");
            stmt.setString(301, gratuity_amt_1 != null ? gratuity_amt_1 : "");
            stmt.setString(302, gratuity_amt_2 != null ? gratuity_amt_2 : "");
            stmt.setString(303, gratuity_amt_3 != null ? gratuity_amt_3 : "");
            stmt.setString(304, gratuity_amt_4 != null ? gratuity_amt_4 : "");
            stmt.setString(305, gratuity_amt_5 != null ? gratuity_amt_5 : "");
            stmt.setString(306, gratuity_amt_6 != null ? gratuity_amt_6 : "");
            stmt.setString(307, gratuity_amt_sum != null ? gratuity_amt_sum : "");
            stmt.setString(308, gratuity_cnt_1 != null ? gratuity_cnt_1 : "");
            stmt.setString(309, gratuity_cnt_2 != null ? gratuity_cnt_2 : "");
            stmt.setString(310, gratuity_cnt_3 != null ? gratuity_cnt_3 : "");
            stmt.setString(311, gratuity_cnt_4 != null ? gratuity_cnt_4 : "");
            stmt.setString(312, gratuity_cnt_5 != null ? gratuity_cnt_5 : "");
            stmt.setString(313, gratuity_cnt_6 != null ? gratuity_cnt_6 : "");
            stmt.setString(314, gratuity_cnt_sum != null ? gratuity_cnt_sum : "");
            stmt.setString(315, pay_for_ohter_amt_1 != null ? pay_for_ohter_amt_1 : "");
            stmt.setString(316, pay_for_ohter_amt_2 != null ? pay_for_ohter_amt_2 : "");
            stmt.setString(317, pay_for_ohter_amt_3 != null ? pay_for_ohter_amt_3 : "");
            stmt.setString(318, pay_for_ohter_amt_4 != null ? pay_for_ohter_amt_4 : "");
            stmt.setString(319, pay_for_ohter_amt_5 != null ? pay_for_ohter_amt_5 : "");
            stmt.setString(320, pay_for_ohter_amt_6 != null ? pay_for_ohter_amt_6 : "");
            stmt.setString(321, pay_for_ohter_amt_sum != null ? pay_for_ohter_amt_sum : "");
            stmt.setString(322, pay_for_ohter_cnt_1 != null ? pay_for_ohter_cnt_1 : "");
            stmt.setString(323, pay_for_ohter_cnt_2 != null ? pay_for_ohter_cnt_2 : "");
            stmt.setString(324, pay_for_ohter_cnt_3 != null ? pay_for_ohter_cnt_3 : "");
            stmt.setString(325, pay_for_ohter_cnt_4 != null ? pay_for_ohter_cnt_4 : "");
            stmt.setString(326, pay_for_ohter_cnt_5 != null ? pay_for_ohter_cnt_5 : "");
            stmt.setString(327, pay_for_ohter_cnt_6 != null ? pay_for_ohter_cnt_6 : "");
            stmt.setString(328, pay_for_ohter_cnt_sum != null ? pay_for_ohter_cnt_sum : "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        int files = 0;
        Connection conn = GetConnection.getConn_Cdsp_Riskcore();
        String sql = "";
        sql = "SELECT a.VOUCHER_NO,a.FILE_PATH,r.CLIENT_NO,r.CLIENT_NAME,r.CERT_NO " +
                "FROM cdsp.cdsp_hulu_access a,riskcore.risk_credit_application r " +
                "WHERE a.QUERY_TYPE = 'MOXIE_ALIPAY_REPORT' AND  a.file_path IS NOT NULL  " +
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
//                filePath = "D:\\tmp\\aliplay_report_0825.json";
                readFileByLines(voucher_no, filePath, client_no, client_name, cert_no);
                System.out.println(DateUtil.nowString() + " ==aliplay=files====" + ++files);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " AliplayReport2Phoenix 导入耗时为： " + (endtime - starttime));
    }


}
