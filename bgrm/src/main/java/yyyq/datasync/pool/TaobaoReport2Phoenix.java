package yyyq.datasync.pool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;

import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaobaoReport2Phoenix implements Runnable {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public String voucher_no = "";
    public String filePath = "";
    public String client_no = "";
    public String client_name = "";
    public String cert_no = "";

    public TaobaoReport2Phoenix(String voucher_no, String filePath, String client_no, String client_name, String cert_no) {
        this.voucher_no = voucher_no;
        this.filePath = filePath;
        this.client_no = client_no;
        this.client_name = client_name;
        this.cert_no = cert_no;
    }

    public void addBuyBean(List<TaobaoReportBean> list) {
        final List<TaobaoReportBean> tempBpplist = list;
        String sql = "upsert  into \"taobao_report_info\" (\"ID\" ,\"taobao_name\",\"taobao_email\",\"taobao_phone_number\",\"alipay_account\",\"taobao_vip_level\",\"taobao_vip_count\",\"tmall_level\",\"tmall_vip_count\",\"tmall_apass\",\"balance\",\"yue_e_bao_amt\",\"total_profit\",\"huai_bei_limit\",\"huai_bei_can_use_limit\",\"self_address_change\",\"self_city_change\",\"nonself_address_change\",\"self_address_cnt\",\"avg_self_address_cnt\",\"self_city_cnt\",\"avg_self_city_cnt\",\"nonself_address_cnt\",\"avg_nonself_address_cnt\",\"deliver_address_1\",\"deliver_address_2\",\"deliver_address_3\",\"deliver_city_1\",\"deliver_city_2\",\"deliver_city_3\",\"deliver_address_type_1\",\"deliver_address_type_2\",\"deliver_address_type_3\",\"use_month_1\",\"use_month_2\",\"use_month_3\",\"last_deliver_past_cur_1\",\"last_deliver_past_cur_2\",\"last_deliver_past_cur_3\",\"first_deliver_time_1\",\"first_deliver_time_2\",\"first_deliver_time_3\",\"last_deliver_time_1\",\"last_deliver_time_2\",\"last_deliver_time_3\",\"deliver_name_1\",\"deliver_name_2\",\"deliver_name_3\",\"deliver_phone_1\",\"deliver_phone_2\",\"deliver_phone_3\",\"deliver_amt_1\",\"deliver_amt_2\",\"deliver_amt_3\",\"deliver_cnt_1\",\"deliver_cnt_2\",\"deliver_cnt_3\",\"receiving_amt_1\",\"receiving_amt_2\",\"receiving_amt_3\",\"receiving_cnt_1\",\"receiving_cnt_2\",\"receiving_cnt_3\",\"max_deliver_name_3\",\"max_deliver_name_6\",\"max_deliver_phone_3\",\"max_deliver_phone_6\",\"max_deliver_address_3\",\"max_deliver_address_6\",\"max_deliver_city_3\",\"max_deliver_city_6\",\"is_default_3\",\"is_default_6\",\"max_cnt_3\",\"max_cnt_6\",\"default_deliver_cnt_3\",\"default_deliver_cnt_6\",\"max_deliver_city_cnt_3\",\"max_deliver_city_cnt_6\",\"total_category_cnt_1\",\"total_category_cnt_2\",\"total_category_cnt_3\",\"total_category_cnt_4\",\"total_category_cnt_5\",\"total_category_cnt_6\",\"total_category_cnt_sum\",\"total_consum_amt_1\",\"total_consum_amt_2\",\"total_consum_amt_3\",\"total_consum_amt_4\",\"total_consum_amt_5\",\"total_consum_amt_6\",\"total_consum_amt_sum\",\"total_consum_times_1\",\"total_consum_times_2\",\"total_consum_times_3\",\"total_consum_times_4\",\"total_consum_times_5\",\"total_consum_times_6\",\"total_consum_times_sum\",\"lottery_amt_1\",\"lottery_amt_2\",\"lottery_amt_3\",\"lottery_amt_4\",\"lottery_amt_5\",\"lottery_amt_6\",\"lottery_amt_sum\",\"lottery_cnt_1\",\"lottery_cnt_2\",\"lottery_cnt_3\",\"lottery_cnt_4\",\"lottery_cnt_5\",\"lottery_cnt_6\",\"lottery_cnt_sum\",\"lottery_rate_1\",\"lottery_rate_2\",\"lottery_rate_3\",\"lottery_rate_4\",\"lottery_rate_5\",\"lottery_rate_6\",\"lottery_rate_sum\",\"virtual_goods_amt_1\",\"virtual_goods_amt_2\",\"virtual_goods_amt_3\",\"virtual_goods_amt_4\",\"virtual_goods_amt_5\",\"virtual_goods_amt_6\",\"virtual_goods_amt_sum\",\"virtual_goods_cnt_1\",\"virtual_goods_cnt_2\",\"virtual_goods_cnt_3\",\"virtual_goods_cnt_4\",\"virtual_goods_cnt_5\",\"virtual_goods_cnt_6\",\"virtual_goods_cnt_sum\",\"virtual_goods_rate_1\",\"virtual_goods_rate_2\",\"virtual_goods_rate_3\",\"virtual_goods_rate_4\",\"virtual_goods_rate_5\",\"virtual_goods_rate_6\",\"virtual_goods_rate_sum\",\"self_category_cnt_1\",\"self_category_cnt_2\",\"self_category_cnt_3\",\"self_category_cnt_4\",\"self_category_cnt_5\",\"self_category_cnt_6\",\"self_category_cnt_sum\",\"self_consum_amt_1\",\"self_consum_amt_2\",\"self_consum_amt_3\",\"self_consum_amt_4\",\"self_consum_amt_5\",\"self_consum_amt_6\",\"self_consum_amt_sum\",\"self_consum_times_1\",\"self_consum_times_2\",\"self_consum_times_3\",\"self_consum_times_4\",\"self_consum_times_5\",\"self_consum_times_6\",\"self_consum_times_sum\",\"loadtime\",\"client_name\",\"cert_no\")values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public int getBatchSize() {
                return tempBpplist.size();
            }
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, tempBpplist.get(i).getAlipay_account());
//                ps.setInt(2, tempBpplist.get(i).getPId());

                try {
                    ps.setString(1, client_no + "_" + voucher_no);
                    ps.setString(2, tempBpplist.get(i).getTaobao_name());
                    ps.setString(3, tempBpplist.get(i).getTaobao_email());
                    ps.setString(4, tempBpplist.get(i).getTaobao_phone_number());
                    ps.setString(5, tempBpplist.get(i).getAlipay_account());
                    ps.setString(6, tempBpplist.get(i).getTaobao_vip_level());
                    ps.setString(7, tempBpplist.get(i).getTaobao_vip_count());
                    ps.setString(8, tempBpplist.get(i).getTmall_level());
                    ps.setString(9, tempBpplist.get(i).getTmall_vip_count());
                    ps.setString(10, tempBpplist.get(i).getTmall_apass());
                    ps.setString(11, tempBpplist.get(i).getBalance());
                    ps.setString(12, tempBpplist.get(i).getYue_e_bao_amt());
                    ps.setString(13, tempBpplist.get(i).getTotal_profit());
                    ps.setString(14, tempBpplist.get(i).getHuai_bei_limit());
                    ps.setString(15, tempBpplist.get(i).getHuai_bei_can_use_limit());

                    ps.setString(164, tempBpplist.get(i).getLoadtime());
                    ps.setString(165, tempBpplist.get(i).getClient_name());
                    ps.setString(166, tempBpplist.get(i).getCert_no());

                    ps.setString(16, tempBpplist.get(i).getSelf_address_change());
                    ps.setString(17, tempBpplist.get(i).getSelf_city_change());
                    ps.setString(18, tempBpplist.get(i).getNonself_address_change());
                    ps.setString(19, tempBpplist.get(i).getSelf_address_cnt());
                    ps.setString(20, tempBpplist.get(i).getAvg_self_address_cnt());
                    ps.setString(21, tempBpplist.get(i).getSelf_city_cnt());
                    ps.setString(22, tempBpplist.get(i).getAvg_self_city_cnt());
                    ps.setString(23, tempBpplist.get(i).getNonself_address_cnt());
                    ps.setString(24, tempBpplist.get(i).getAvg_nonself_address_cnt());
                    ps.setString(25, tempBpplist.get(i).getDeliver_address_1());
                    ps.setString(26, tempBpplist.get(i).getDeliver_address_2());
                    ps.setString(27, tempBpplist.get(i).getDeliver_address_3());
                    ps.setString(28, tempBpplist.get(i).getDeliver_city_1());
                    ps.setString(29, tempBpplist.get(i).getDeliver_city_2());
                    ps.setString(30, tempBpplist.get(i).getDeliver_city_3());
                    ps.setString(31, tempBpplist.get(i).getDeliver_address_type_1());
                    ps.setString(32, tempBpplist.get(i).getDeliver_address_type_2());
                    ps.setString(33, tempBpplist.get(i).getDeliver_address_type_3());
                    ps.setString(34, tempBpplist.get(i).getUse_month_1());
                    ps.setString(35, tempBpplist.get(i).getUse_month_2());
                    ps.setString(36, tempBpplist.get(i).getUse_month_3());
                    ps.setString(37, tempBpplist.get(i).getLast_deliver_past_cur_1());
                    ps.setString(38, tempBpplist.get(i).getLast_deliver_past_cur_2());
                    ps.setString(39, tempBpplist.get(i).getLast_deliver_past_cur_3());
                    ps.setString(40, tempBpplist.get(i).getFirst_deliver_time_1());
                    ps.setString(41, tempBpplist.get(i).getFirst_deliver_time_2());
                    ps.setString(42, tempBpplist.get(i).getFirst_deliver_time_3());
                    ps.setString(43, tempBpplist.get(i).getLast_deliver_time_1());
                    ps.setString(44, tempBpplist.get(i).getLast_deliver_time_2());
                    ps.setString(45, tempBpplist.get(i).getLast_deliver_time_3());
                    ps.setString(46, tempBpplist.get(i).getDeliver_name_1());
                    ps.setString(47, tempBpplist.get(i).getDeliver_name_2());
                    ps.setString(48, tempBpplist.get(i).getDeliver_name_3());
                    ps.setString(49, tempBpplist.get(i).getDeliver_phone_1());
                    ps.setString(50, tempBpplist.get(i).getDeliver_phone_2());
                    ps.setString(51, tempBpplist.get(i).getDeliver_phone_3());
                    ps.setString(52, tempBpplist.get(i).getDeliver_amt_1());
                    ps.setString(53, tempBpplist.get(i).getDeliver_amt_2());
                    ps.setString(54, tempBpplist.get(i).getDeliver_amt_3());
                    ps.setString(55, tempBpplist.get(i).getDeliver_cnt_1());
                    ps.setString(56, tempBpplist.get(i).getDeliver_cnt_2());
                    ps.setString(57, tempBpplist.get(i).getDeliver_cnt_3());
                    ps.setString(58, tempBpplist.get(i).getReceiving_amt_1());
                    ps.setString(59, tempBpplist.get(i).getReceiving_amt_2());
                    ps.setString(60, tempBpplist.get(i).getReceiving_amt_3());
                    ps.setString(61, tempBpplist.get(i).getReceiving_cnt_1());
                    ps.setString(62, tempBpplist.get(i).getReceiving_cnt_2());
                    ps.setString(63, tempBpplist.get(i).getReceiving_cnt_3());
                    ps.setString(64, tempBpplist.get(i).getMax_deliver_name_3());
                    ps.setString(65, tempBpplist.get(i).getMax_deliver_name_6());
                    ps.setString(66, tempBpplist.get(i).getMax_deliver_phone_3());
                    ps.setString(67, tempBpplist.get(i).getMax_deliver_phone_6());
                    ps.setString(68, tempBpplist.get(i).getMax_deliver_address_3());
                    ps.setString(69, tempBpplist.get(i).getMax_deliver_address_6());
                    ps.setString(70, tempBpplist.get(i).getMax_deliver_city_3());
                    ps.setString(71, tempBpplist.get(i).getMax_deliver_city_6());
                    ps.setString(72, tempBpplist.get(i).getIs_default_3());
                    ps.setString(73, tempBpplist.get(i).getIs_default_6());
                    ps.setString(74, tempBpplist.get(i).getMax_cnt_3());
                    ps.setString(75, tempBpplist.get(i).getMax_cnt_6());
                    ps.setString(76, tempBpplist.get(i).getDefault_deliver_cnt_3());
                    ps.setString(77, tempBpplist.get(i).getDefault_deliver_cnt_6());
                    ps.setString(78, tempBpplist.get(i).getMax_deliver_city_cnt_3());
                    ps.setString(79, tempBpplist.get(i).getMax_deliver_city_cnt_6());

                    ps.setString(80, tempBpplist.get(i).getTotal_category_cnt_1());
                    ps.setString(81, tempBpplist.get(i).getTotal_category_cnt_2());
                    ps.setString(82, tempBpplist.get(i).getTotal_category_cnt_3());
                    ps.setString(83, tempBpplist.get(i).getTotal_category_cnt_4());
                    ps.setString(84, tempBpplist.get(i).getTotal_category_cnt_5());
                    ps.setString(85, tempBpplist.get(i).getTotal_category_cnt_6());
                    ps.setString(86, tempBpplist.get(i).getTotal_category_cnt_sum());
                    ps.setString(87, tempBpplist.get(i).getTotal_consum_amt_1());
                    ps.setString(88, tempBpplist.get(i).getTotal_consum_amt_2());
                    ps.setString(89, tempBpplist.get(i).getTotal_consum_amt_3());
                    ps.setString(90, tempBpplist.get(i).getTotal_consum_amt_4());
                    ps.setString(91, tempBpplist.get(i).getTotal_consum_amt_5());
                    ps.setString(92, tempBpplist.get(i).getTotal_consum_amt_6());
                    ps.setString(93, tempBpplist.get(i).getTotal_consum_amt_sum());
                    ps.setString(94, tempBpplist.get(i).getTotal_consum_times_1());
                    ps.setString(95, tempBpplist.get(i).getTotal_consum_times_2());
                    ps.setString(96, tempBpplist.get(i).getTotal_consum_times_3());
                    ps.setString(97, tempBpplist.get(i).getTotal_consum_times_4());
                    ps.setString(98, tempBpplist.get(i).getTotal_consum_times_5());
                    ps.setString(99, tempBpplist.get(i).getTotal_consum_times_6());
                    ps.setString(100, tempBpplist.get(i).getTotal_consum_times_sum());

                    ps.setString(101, tempBpplist.get(i).getLottery_amt_1());
                    ps.setString(102, tempBpplist.get(i).getLottery_amt_2());
                    ps.setString(103, tempBpplist.get(i).getLottery_amt_3());
                    ps.setString(104, tempBpplist.get(i).getLottery_amt_4());
                    ps.setString(105, tempBpplist.get(i).getLottery_amt_5());
                    ps.setString(106, tempBpplist.get(i).getLottery_amt_6());
                    ps.setString(107, tempBpplist.get(i).getLottery_amt_sum());
                    ps.setString(108, tempBpplist.get(i).getLottery_cnt_1());
                    ps.setString(109, tempBpplist.get(i).getLottery_cnt_2());
                    ps.setString(110, tempBpplist.get(i).getLottery_cnt_3());
                    ps.setString(111, tempBpplist.get(i).getLottery_cnt_4());
                    ps.setString(112, tempBpplist.get(i).getLottery_cnt_5());
                    ps.setString(113, tempBpplist.get(i).getLottery_cnt_6());
                    ps.setString(114, tempBpplist.get(i).getLottery_cnt_sum());
                    ps.setString(115, tempBpplist.get(i).getLottery_rate_1());
                    ps.setString(116, tempBpplist.get(i).getLottery_rate_2());
                    ps.setString(117, tempBpplist.get(i).getLottery_rate_3());
                    ps.setString(118, tempBpplist.get(i).getLottery_rate_4());
                    ps.setString(119, tempBpplist.get(i).getLottery_rate_5());
                    ps.setString(120, tempBpplist.get(i).getLottery_rate_6());
                    ps.setString(121, tempBpplist.get(i).getLottery_rate_sum());
                    ps.setString(122, tempBpplist.get(i).getVirtual_goods_amt_1());
                    ps.setString(123, tempBpplist.get(i).getVirtual_goods_amt_2());
                    ps.setString(124, tempBpplist.get(i).getVirtual_goods_amt_3());
                    ps.setString(125, tempBpplist.get(i).getVirtual_goods_amt_4());
                    ps.setString(126, tempBpplist.get(i).getVirtual_goods_amt_5());
                    ps.setString(127, tempBpplist.get(i).getVirtual_goods_amt_6());
                    ps.setString(128, tempBpplist.get(i).getVirtual_goods_amt_sum());
                    ps.setString(129, tempBpplist.get(i).getVirtual_goods_cnt_1());
                    ps.setString(130, tempBpplist.get(i).getVirtual_goods_cnt_2());
                    ps.setString(131, tempBpplist.get(i).getVirtual_goods_cnt_3());
                    ps.setString(132, tempBpplist.get(i).getVirtual_goods_cnt_4());
                    ps.setString(133, tempBpplist.get(i).getVirtual_goods_cnt_5());
                    ps.setString(134, tempBpplist.get(i).getVirtual_goods_cnt_6());
                    ps.setString(135, tempBpplist.get(i).getVirtual_goods_cnt_sum());
                    ps.setString(136, tempBpplist.get(i).getVirtual_goods_rate_1());
                    ps.setString(137, tempBpplist.get(i).getVirtual_goods_rate_2());
                    ps.setString(138, tempBpplist.get(i).getVirtual_goods_rate_3());
                    ps.setString(139, tempBpplist.get(i).getVirtual_goods_rate_4());
                    ps.setString(140, tempBpplist.get(i).getVirtual_goods_rate_5());
                    ps.setString(141, tempBpplist.get(i).getVirtual_goods_rate_6());
                    ps.setString(142, tempBpplist.get(i).getVirtual_goods_rate_sum());

                    ps.setString(143, tempBpplist.get(i).getSelf_category_cnt_1());
                    ps.setString(144, tempBpplist.get(i).getSelf_category_cnt_2());
                    ps.setString(145, tempBpplist.get(i).getSelf_category_cnt_3());
                    ps.setString(146, tempBpplist.get(i).getSelf_category_cnt_4());
                    ps.setString(147, tempBpplist.get(i).getSelf_category_cnt_5());
                    ps.setString(148, tempBpplist.get(i).getSelf_category_cnt_6());
                    ps.setString(149, tempBpplist.get(i).getSelf_category_cnt_sum());
                    ps.setString(150, tempBpplist.get(i).getSelf_consum_amt_1());
                    ps.setString(151, tempBpplist.get(i).getSelf_consum_amt_2());
                    ps.setString(152, tempBpplist.get(i).getSelf_consum_amt_3());
                    ps.setString(153, tempBpplist.get(i).getSelf_consum_amt_4());
                    ps.setString(154, tempBpplist.get(i).getSelf_consum_amt_5());
                    ps.setString(155, tempBpplist.get(i).getSelf_consum_amt_6());
                    ps.setString(156, tempBpplist.get(i).getSelf_consum_amt_sum());
                    ps.setString(157, tempBpplist.get(i).getSelf_consum_times_1());
                    ps.setString(158, tempBpplist.get(i).getSelf_consum_times_2());
                    ps.setString(159, tempBpplist.get(i).getSelf_consum_times_3());
                    ps.setString(160, tempBpplist.get(i).getSelf_consum_times_4());
                    ps.setString(161, tempBpplist.get(i).getSelf_consum_times_5());
                    ps.setString(162, tempBpplist.get(i).getSelf_consum_times_6());
                    ps.setString(163, tempBpplist.get(i).getSelf_consum_times_sum());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void run() {
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

                        TaobaoReportBean taobaoReportBean = new TaobaoReportBean();
                        taobaoReportBean.setTaobao_name(taobao_name);
                        taobaoReportBean.setTaobao_email(taobao_email);
                        taobaoReportBean.setTaobao_phone_number(taobao_phone_number);
                        taobaoReportBean.setAlipay_account(alipay_account);
                        taobaoReportBean.setTaobao_vip_level(taobao_vip_level);
                        taobaoReportBean.setTaobao_vip_count(taobao_vip_count);
                        taobaoReportBean.setTmall_level(tmall_level);
                        taobaoReportBean.setTmall_vip_count(tmall_vip_count);
                        taobaoReportBean.setTmall_apass(tmall_apass);
                        taobaoReportBean.setBalance(balance);
                        taobaoReportBean.setYue_e_bao_amt(yue_e_bao_amt);
                        taobaoReportBean.setTotal_profit(total_profit);
                        taobaoReportBean.setHuai_bei_limit(huai_bei_limit);
                        taobaoReportBean.setHuai_bei_can_use_limit(huai_bei_can_use_limit);
                        taobaoReportBean.setLoadtime(DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                        taobaoReportBean.setClient_name(client_name);
                        taobaoReportBean.setCert_no(cert_no);

                        JSONObject consumption_analysis = (JSONObject) jobj.get("consumption_analysis");
                        //  类目一
                        category_one(taobaoReportBean, consumption_analysis);
                        // 类目二
                        category_two(taobaoReportBean, consumption_analysis);
                        // 类目三
                        category_three(taobaoReportBean, consumption_analysis);
                        // 类目四
                        JSONObject address_analysis = (JSONObject) jobj.get("address_analysis");
                        category_four(taobaoReportBean, address_analysis);

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

    private static void category_four(TaobaoReportBean taobaoReportBean, JSONObject address_analysis) {
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
        taobaoReportBean.setSelf_address_change(self_address_change);
        taobaoReportBean.setSelf_city_change(self_city_change);
        taobaoReportBean.setNonself_address_change(nonself_address_change);
        taobaoReportBean.setSelf_address_cnt(self_address_cnt);
        taobaoReportBean.setAvg_self_address_cnt(avg_self_address_cnt);
        taobaoReportBean.setSelf_city_cnt(self_city_cnt);
        taobaoReportBean.setAvg_self_city_cnt(avg_self_city_cnt);
        taobaoReportBean.setNonself_address_cnt(nonself_address_cnt);
        taobaoReportBean.setAvg_nonself_address_cnt(avg_nonself_address_cnt);

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
        taobaoReportBean.setDeliver_address_1(deliver_address_1);
        taobaoReportBean.setDeliver_address_2(deliver_address_2);
        taobaoReportBean.setDeliver_address_3(deliver_address_3);

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
        taobaoReportBean.setDeliver_city_1(deliver_city_1);
        taobaoReportBean.setDeliver_city_2(deliver_city_2);
        taobaoReportBean.setDeliver_city_3(deliver_city_3);

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
        taobaoReportBean.setDeliver_address_type_1(deliver_address_type_1);
        taobaoReportBean.setDeliver_address_type_2(deliver_address_type_2);
        taobaoReportBean.setDeliver_address_type_3(deliver_address_type_3);

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
        taobaoReportBean.setUse_month_1(use_month_1);
        taobaoReportBean.setUse_month_2(use_month_2);
        taobaoReportBean.setUse_month_3(use_month_3);

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
        taobaoReportBean.setLast_deliver_past_cur_1(last_deliver_past_cur_1);
        taobaoReportBean.setLast_deliver_past_cur_2(last_deliver_past_cur_2);
        taobaoReportBean.setLast_deliver_past_cur_3(last_deliver_past_cur_3);

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
        taobaoReportBean.setFirst_deliver_time_1(first_deliver_time_1);
        taobaoReportBean.setFirst_deliver_time_2(first_deliver_time_2);
        taobaoReportBean.setFirst_deliver_time_3(first_deliver_time_3);

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
        taobaoReportBean.setLast_deliver_time_1(last_deliver_time_1);
        taobaoReportBean.setLast_deliver_time_2(last_deliver_time_2);
        taobaoReportBean.setLast_deliver_time_3(last_deliver_time_3);

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
        taobaoReportBean.setDeliver_name_1(deliver_name_1);
        taobaoReportBean.setDeliver_name_2(deliver_name_2);
        taobaoReportBean.setDeliver_name_3(deliver_name_3);

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
        taobaoReportBean.setDeliver_phone_1(deliver_phone_1);
        taobaoReportBean.setDeliver_phone_2(deliver_phone_2);
        taobaoReportBean.setDeliver_phone_3(deliver_phone_3);

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
        taobaoReportBean.setDeliver_amt_1(deliver_amt_1);
        taobaoReportBean.setDeliver_amt_2(deliver_amt_2);
        taobaoReportBean.setDeliver_amt_3(deliver_amt_3);

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
        taobaoReportBean.setDeliver_cnt_1(deliver_cnt_1);
        taobaoReportBean.setDeliver_cnt_2(deliver_cnt_2);
        taobaoReportBean.setDeliver_cnt_3(deliver_cnt_3);

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
        taobaoReportBean.setReceiving_amt_1(receiving_amt_1);
        taobaoReportBean.setReceiving_amt_2(receiving_amt_2);
        taobaoReportBean.setReceiving_amt_3(receiving_amt_3);

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
        taobaoReportBean.setReceiving_cnt_1(receiving_cnt_1);
        taobaoReportBean.setReceiving_cnt_2(receiving_cnt_2);
        taobaoReportBean.setReceiving_cnt_3(receiving_cnt_3);

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
        taobaoReportBean.setMax_deliver_name_3(max_deliver_name_3);
        taobaoReportBean.setMax_deliver_name_6(max_deliver_name_6);
        taobaoReportBean.setMax_deliver_phone_3(max_deliver_phone_3);
        taobaoReportBean.setMax_deliver_phone_6(max_deliver_phone_6);
        taobaoReportBean.setMax_deliver_address_3(max_deliver_address_3);
        taobaoReportBean.setMax_deliver_address_6(max_deliver_address_6);
        taobaoReportBean.setMax_deliver_city_3(max_deliver_city_3);
        taobaoReportBean.setMax_deliver_city_6(max_deliver_city_6);
        taobaoReportBean.setIs_default_3(is_default_3);
        taobaoReportBean.setIs_default_6(is_default_6);
        taobaoReportBean.setMax_cnt_3(max_cnt_3);
        taobaoReportBean.setMax_cnt_6(max_cnt_6);
        taobaoReportBean.setDefault_deliver_cnt_3(default_deliver_cnt_3);
        taobaoReportBean.setDefault_deliver_cnt_6(default_deliver_cnt_6);
        taobaoReportBean.setMax_deliver_city_cnt_3(max_deliver_city_cnt_3);
        taobaoReportBean.setMax_deliver_city_cnt_6(max_deliver_city_cnt_6);

        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void category_three(TaobaoReportBean taobaoReportBean, JSONObject consumption_analysis) {
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
        taobaoReportBean.setSelf_category_cnt_1(self_category_cnt_1);
        taobaoReportBean.setSelf_category_cnt_2(self_category_cnt_2);
        taobaoReportBean.setSelf_category_cnt_3(self_category_cnt_3);
        taobaoReportBean.setSelf_category_cnt_4(self_category_cnt_4);
        taobaoReportBean.setSelf_category_cnt_5(self_category_cnt_5);
        taobaoReportBean.setSelf_category_cnt_6(self_category_cnt_6);
        taobaoReportBean.setSelf_category_cnt_sum(self_category_cnt_sum);

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
        taobaoReportBean.setSelf_consum_amt_1(self_consum_amt_1);
        taobaoReportBean.setSelf_consum_amt_2(self_consum_amt_2);
        taobaoReportBean.setSelf_consum_amt_3(self_consum_amt_3);
        taobaoReportBean.setSelf_consum_amt_4(self_consum_amt_4);
        taobaoReportBean.setSelf_consum_amt_5(self_consum_amt_5);
        taobaoReportBean.setSelf_consum_amt_6(self_consum_amt_6);
        taobaoReportBean.setSelf_consum_amt_sum(self_consum_amt_sum);

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
        taobaoReportBean.setSelf_consum_times_1(self_consum_times_1);
        taobaoReportBean.setSelf_consum_times_2(self_consum_times_2);
        taobaoReportBean.setSelf_consum_times_3(self_consum_times_3);
        taobaoReportBean.setSelf_consum_times_4(self_consum_times_4);
        taobaoReportBean.setSelf_consum_times_5(self_consum_times_5);
        taobaoReportBean.setSelf_consum_times_6(self_consum_times_6);
        taobaoReportBean.setSelf_consum_times_sum(self_consum_times_sum);
    }

    private static void category_two(TaobaoReportBean taobaoReportBean, JSONObject consumption_analysis) {
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
        taobaoReportBean.setLottery_amt_1(lottery_amt_1);
        taobaoReportBean.setLottery_amt_2(lottery_amt_2);
        taobaoReportBean.setLottery_amt_3(lottery_amt_3);
        taobaoReportBean.setLottery_amt_4(lottery_amt_4);
        taobaoReportBean.setLottery_amt_5(lottery_amt_5);
        taobaoReportBean.setLottery_amt_6(lottery_amt_6);
        taobaoReportBean.setLottery_amt_sum(lottery_amt_sum);

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
        taobaoReportBean.setLottery_cnt_1(lottery_cnt_1);
        taobaoReportBean.setLottery_cnt_2(lottery_cnt_2);
        taobaoReportBean.setLottery_cnt_3(lottery_cnt_3);
        taobaoReportBean.setLottery_cnt_4(lottery_cnt_4);
        taobaoReportBean.setLottery_cnt_5(lottery_cnt_5);
        taobaoReportBean.setLottery_cnt_6(lottery_cnt_6);
        taobaoReportBean.setLottery_cnt_sum(lottery_cnt_sum);

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
        taobaoReportBean.setLottery_rate_1(lottery_rate_1);
        taobaoReportBean.setLottery_rate_2(lottery_rate_2);
        taobaoReportBean.setLottery_rate_3(lottery_rate_3);
        taobaoReportBean.setLottery_rate_4(lottery_rate_4);
        taobaoReportBean.setLottery_rate_5(lottery_rate_5);
        taobaoReportBean.setLottery_rate_6(lottery_rate_6);
        taobaoReportBean.setLottery_rate_sum(lottery_rate_sum);

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
        taobaoReportBean.setVirtual_goods_amt_1(virtual_goods_amt_1);
        taobaoReportBean.setVirtual_goods_amt_2(virtual_goods_amt_2);
        taobaoReportBean.setVirtual_goods_amt_3(virtual_goods_amt_3);
        taobaoReportBean.setVirtual_goods_amt_4(virtual_goods_amt_4);
        taobaoReportBean.setVirtual_goods_amt_5(virtual_goods_amt_5);
        taobaoReportBean.setVirtual_goods_amt_6(virtual_goods_amt_6);
        taobaoReportBean.setVirtual_goods_amt_sum(virtual_goods_amt_sum);

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
        taobaoReportBean.setVirtual_goods_cnt_1(virtual_goods_cnt_1);
        taobaoReportBean.setVirtual_goods_cnt_2(virtual_goods_cnt_2);
        taobaoReportBean.setVirtual_goods_cnt_3(virtual_goods_cnt_3);
        taobaoReportBean.setVirtual_goods_cnt_4(virtual_goods_cnt_4);
        taobaoReportBean.setVirtual_goods_cnt_5(virtual_goods_cnt_5);
        taobaoReportBean.setVirtual_goods_cnt_6(virtual_goods_cnt_6);
        taobaoReportBean.setVirtual_goods_cnt_sum(virtual_goods_cnt_sum);

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
        taobaoReportBean.setVirtual_goods_rate_1(virtual_goods_rate_1);
        taobaoReportBean.setVirtual_goods_rate_2(virtual_goods_rate_2);
        taobaoReportBean.setVirtual_goods_rate_3(virtual_goods_rate_3);
        taobaoReportBean.setVirtual_goods_rate_4(virtual_goods_rate_4);
        taobaoReportBean.setVirtual_goods_rate_5(virtual_goods_rate_5);
        taobaoReportBean.setVirtual_goods_rate_6(virtual_goods_rate_6);
        taobaoReportBean.setVirtual_goods_rate_sum(virtual_goods_rate_sum);

    }

    private static void category_one(TaobaoReportBean taobaoReportBean, JSONObject consumption_analysis) {
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
        taobaoReportBean.setTotal_category_cnt_1(total_category_cnt_1);
        taobaoReportBean.setTotal_category_cnt_2(total_category_cnt_2);
        taobaoReportBean.setTotal_category_cnt_3(total_category_cnt_3);
        taobaoReportBean.setTotal_category_cnt_4(total_category_cnt_4);
        taobaoReportBean.setTotal_category_cnt_5(total_category_cnt_5);
        taobaoReportBean.setTotal_category_cnt_6(total_category_cnt_6);
        taobaoReportBean.setTotal_category_cnt_sum(total_category_cnt_sum);

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
        taobaoReportBean.setTotal_consum_amt_1(total_consum_amt_1);
        taobaoReportBean.setTotal_consum_amt_2(total_consum_amt_2);
        taobaoReportBean.setTotal_consum_amt_3(total_consum_amt_3);
        taobaoReportBean.setTotal_consum_amt_4(total_consum_amt_4);
        taobaoReportBean.setTotal_consum_amt_5(total_consum_amt_5);
        taobaoReportBean.setTotal_consum_amt_6(total_consum_amt_6);
        taobaoReportBean.setTotal_consum_amt_sum(total_consum_amt_sum);

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
        taobaoReportBean.setTotal_consum_times_1(total_consum_times_1);
        taobaoReportBean.setTotal_consum_times_2(total_consum_times_2);
        taobaoReportBean.setTotal_consum_times_3(total_consum_times_3);
        taobaoReportBean.setTotal_consum_times_4(total_consum_times_4);
        taobaoReportBean.setTotal_consum_times_5(total_consum_times_5);
        taobaoReportBean.setTotal_consum_times_6(total_consum_times_6);
        taobaoReportBean.setTotal_consum_times_sum(total_consum_times_sum);

    }

}


class ThreadPool {

    public static ExecutorService executor;

    public ThreadPool() {
        this.executor = Executors.newFixedThreadPool(10);
    }

    public static void main(String[] args) {
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
//                readFileByLines(voucher_no, filePath, client_no, client_name, cert_no);
                executor.submit(new TaobaoReport2Phoenix(voucher_no, filePath, client_no, client_name, cert_no));
                System.out.println(DateUtil.nowString() + " ==DS_report=files====" + ++files + "===filepath===" + filePath);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " 电商report导入耗时为： " + (endtime - starttime));
    }

}
