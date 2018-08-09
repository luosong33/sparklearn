package yyyq.datasync.xiaobo.d_0302;

import yyyq.util.DateUtil;
import yyyq.util.ReadWriteUtil;
import yyyq.util.druidOrPool.DruidUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/*
    读取7万个逾期用户，查询其通讯录后，写入到文件，作准备导入
*/
public class XiaoboDetail_Sin {

    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();

        ArrayList<String> list = handle("data/0302.txt", "UTF-8");  //  rpt_xjb_data_test
        ArrayList<String> _list = new ArrayList<>();
        int files = 0;
        for (String s : list) {
            files++;
            _list.add(s);
            if (files % 5000 == 0) {
                ArrayList<String> list_ = new ArrayList<>();
                list_.addAll(_list);
                _list.clear();
                quere(list_);
                System.out.println(DateUtil.nowString() + " =======" + files + "=======");
            }
        }
        quere(_list);
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " OverdueCellDetail 耗时为： " + (endtime - starttime));
    }

    public static ArrayList<String> handle(String inpath, String fileEncoding) {
        ArrayList<String> list = new ArrayList<>();
        try {
            FileInputStream inputStream = new FileInputStream(inpath);
            //  字节流中指定编码
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
            String s = null;
            int i = 0;
            while ((s = bufferedReader.readLine()) != null) {
                String[] split = s.split(",", -1);
                String phoen = split[1];
                if (!"".equals(phoen)) {
                    list.add(phoen);
                }
                i++;
                if (i % 10000 == 0) {
                    System.out.println("========================" + i + "========================");
                }
            }
            System.out.println("========================" + i + "========================");
            inputStream.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static void quere(ArrayList<String> list_) {
        System.out.println(Thread.currentThread() + "================Run================" + list_.size() + "================Run================");
        Connection conn = DruidUtil.getConn();
        PreparedStatement pstmt;

        for (String s : list_) {
            String sql_ = "select * from \"capricorn_report_call_contact_detail\" where ROW like '" + s + "%'";
            try {
                pstmt = conn.prepareStatement(sql_);
                ResultSet rs_ = pstmt.executeQuery();
                while (rs_.next()) {
                    String ROW = rs_.getString("ROW");   // 客户编号
                    String call_cnt_6m = rs_.getString("call_cnt_6m");   // 近6月（最近0-180天）通话次数
                    String call_time_6m = rs_.getString("call_time_6m");   // 近6月通话时长
                    StringBuffer sb = new StringBuffer();
                    sb.append(ROW + ",").append(call_cnt_6m + ",").append(call_time_6m);
                    ReadWriteUtil.write("E:\\ls\\xiaobo\\0302.txt", sb.toString());
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
