package yyyq.datasync.jingshuang;

import yyyq.util.DateUtil;
import yyyq.util.GetConnection;
import yyyq.util.ReadWriteUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/* 与user表撞到的装了盈盈易贷的用户  查询他们的通讯录 */
public class YyydInfo2Phoenix {

    public static void main(String[] args) {
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

                String sql_ = "select * from \"ods_cell_linker\" where ID like '"+ client_no +"%'";
                pstmt = conn.prepareStatement(sql_);
                ResultSet rs_ = pstmt.executeQuery();
                while (rs_.next()) {
                    String clientNo = rs_.getString("clientNo");   // 客户编号
                    String linkPhone = rs_.getString("linkPhone");   // 客户编号
                    String linker = rs_.getString("linker");   // 客户编号
                    StringBuffer sb = new StringBuffer();
                    sb.append(clientNo+",").append(linkPhone+",").append(linker);
                    ReadWriteUtil.write("E:\\ls\\jingshuang\\yyyd_info_1207.txt",sb.toString());
                }
                System.out.println(DateUtil.nowString()+" =======" + ++files+"==client_no=="+client_no);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString()+" SAURON_ACCESS_SUMMIT 耗时为： " + (endtime - starttime));
    }

}
