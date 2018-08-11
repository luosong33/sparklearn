package yyyq.datasync.jingshuang;

import jpype.AESDecrypt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;
import yyyq.util.ReadWriteUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/* 与user表撞到的装了盈盈易贷的用户  查询他们的通讯详情 */
public class YyydInfoReport {

    @Autowired
    private static JdbcTemplate jdbcTemplate;

    /*public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"applicationContext.xml"});
        JdbcTemplate dataSource = (JdbcTemplate) context.getBean("dataSource");
        JdbcTemplate jdbcTemplate = (JdbcTemplate) context.getBean("jdbcTemplate");

        long starttime = System.currentTimeMillis();
        int files = 0;
        Connection conn = GetConnection.getPhoenixConn();
        String sql = "select \"client_no\" from \"yyyd_info_user\"";
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String client_no = rs.getString("client_no");

                String sql_ = "select * from \"capricorn_report_call_contact_detail\" where ID like '" + client_no + "%'";
                *//*YyydInfoEntity yyydInfoEntity = (YyydInfoEntity) jdbcTemplate.queryForObject(
                        "select * from channel.Dg_Pdj_Day_Report order by p_id desc", new BeanPropertyRowMapper(YyydInfoEntity.class));*//*
                List<YyydInfoEntity> list = jdbcTemplate.queryForList(sql_, YyydInfoEntity.class);
                for (YyydInfoEntity yyydInfoEntity : list) {
                    ReadWriteUtil.write("E:\\ls\\jingshuang\\yyyd_info_0102.txt", yyydInfoEntity.toString());
                }
                System.out.println(DateUtil.nowString() + " =======" + ++files + "==client_no==" + client_no);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " SAURON_ACCESS_SUMMIT 耗时为： " + (endtime - starttime));

    }*/

    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();
        int files = 0;
        Connection conn = GetConnection.getPhoenixConn();
        String sql = "select client_no,mobile from \"YYYD_INFO_USER\"";
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String client_no = rs.getString("client_no");
                String mobile = rs.getString("mobile");
                mobile = new AESDecrypt().getDecrypt(mobile);
                String sql_ = "select * from \"capricorn_report_call_contact_detail\" where ROW like '"+ mobile +"%'";
                pstmt = conn.prepareStatement(sql_);
                ResultSet rs_ = pstmt.executeQuery();
                while (rs_.next()) {
                    String city = rs_.getString("city");
                    String p_relation = rs_.getString("p_relation");
                    String mobile_answer = rs_.getString("mobile_answer");
                    String group_name = rs_.getString("group_name");
                    String company_name = rs_.getString("company_name");
                    String call_cnt_1w = rs_.getString("call_cnt_1w");
                    String call_cnt_1m = rs_.getString("call_cnt_1m");
                    String call_cnt_3m = rs_.getString("call_cnt_3m");
                    String call_cnt_6m = rs_.getString("call_cnt_6m");
                    String call_time_3m = rs_.getString("call_time_3m");
                    String call_time_6m = rs_.getString("call_time_6m");
                    String dial_cnt_3m = rs_.getString("dial_cnt_3m");
                    String dial_cnt_6m = rs_.getString("dial_cnt_6m");
                    String dial_time_3m = rs_.getString("dial_time_3m");
                    String dial_time_6m = rs_.getString("dial_time_6m");
                    String dialed_cnt_3m = rs_.getString("dialed_cnt_3m");
                    String dialed_cnt_6m = rs_.getString("dialed_cnt_6m");
                    String dialed_time_3m = rs_.getString("dialed_time_3m");
                    String dialed_time_6m = rs_.getString("dialed_time_6m");
                    String call_cnt_morning_3m = rs_.getString("call_cnt_morning_3m");
                    String call_cnt_morning_6m = rs_.getString("call_cnt_morning_6m");
                    String call_cnt_noon_3m = rs_.getString("call_cnt_noon_3m");
                    String call_cnt_noon_6m = rs_.getString("call_cnt_noon_6m");
                    String call_cnt_afternoon_3m = rs_.getString("call_cnt_afternoon_3m");
                    String call_cnt_afternoon_6m = rs_.getString("call_cnt_afternoon_6m");
                    String call_cnt_evening_3m = rs_.getString("call_cnt_evening_3m");
                    String call_cnt_evening_6m = rs_.getString("call_cnt_evening_6m");
                    String call_cnt_night_3m = rs_.getString("call_cnt_night_3m");
                    String call_cnt_night_6m = rs_.getString("call_cnt_night_6m");
                    String call_cnt_weekday_3m = rs_.getString("call_cnt_weekday_3m");
                    String call_cnt_weekday_6m = rs_.getString("call_cnt_weekday_6m");
                    String call_cnt_weekend_3m = rs_.getString("call_cnt_weekend_3m");
                    String call_cnt_weekend_6m = rs_.getString("call_cnt_weekend_6m");
                    String call_cnt_holiday_3m = rs_.getString("call_cnt_holiday_3m");
                    String call_cnt_holiday_6m = rs_.getString("call_cnt_holiday_6m");
                    String call_if_whole_day_3m = rs_.getString("call_if_whole_day_3m");
                    String call_if_whole_day_6m = rs_.getString("call_if_whole_day_6m");
                    String trans_start = rs_.getString("trans_start");
                    String trans_end = rs_.getString("trans_end");
                    String mobile_local = rs_.getString("mobile_local");
                    String voucher_no = rs_.getString("voucher_no");
                    StringBuffer sb = new StringBuffer();
                    sb.append(client_no+"\t")
                            .append(mobile_local+"\t")
                            .append(mobile_answer+"\t")
                            .append(voucher_no+"\t")
                            .append(city+"\t")
                            .append(p_relation+"\t")
                            .append(group_name+"\t")
                            .append(company_name+"\t")
                            .append(call_cnt_1w+"\t")
                            .append(call_cnt_1m+"\t")
                            .append(call_cnt_3m+"\t")
                            .append(call_cnt_6m+"\t")
                            .append(call_time_3m+"\t")
                            .append(call_time_6m+"\t")
                            .append(dial_cnt_3m+"\t")
                            .append(dial_cnt_6m+"\t")
                            .append(dial_time_3m+"\t")
                            .append(dial_time_6m+"\t")
                            .append(dialed_cnt_3m+"\t")
                            .append(dialed_cnt_6m+"\t")
                            .append(dialed_time_3m+"\t")
                            .append(dialed_time_6m+"\t")
                            .append(call_cnt_morning_3m+"\t")
                            .append(call_cnt_morning_6m+"\t")
                            .append(call_cnt_noon_3m+"\t")
                            .append(call_cnt_noon_6m+"\t")
                            .append(call_cnt_afternoon_3m+"\t")
                            .append(call_cnt_afternoon_6m+"\t")
                            .append(call_cnt_evening_3m+"\t")
                            .append(call_cnt_evening_6m+"\t")
                            .append(call_cnt_night_3m+"\t")
                            .append(call_cnt_night_6m+"\t")
                            .append(call_cnt_weekday_3m+"\t")
                            .append(call_cnt_weekday_6m+"\t")
                            .append(call_cnt_weekend_3m+"\t")
                            .append(call_cnt_weekend_6m+"\t")
                            .append(call_cnt_holiday_3m+"\t")
                            .append(call_cnt_holiday_6m+"\t")
                            .append(call_if_whole_day_3m+"\t")
                            .append(call_if_whole_day_6m+"\t")
                            .append(trans_start+"\t")
                            .append(trans_end);
                    ReadWriteUtil.write("E:\\ls\\jingshuang\\yyyd_info_0103.txt",sb.toString());
                }
                System.out.println(DateUtil.nowString()+" =======" + ++files+"==mobile=="+mobile);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString()+" SAURON_ACCESS_SUMMIT 耗时为： " + (endtime - starttime));
    }

}
