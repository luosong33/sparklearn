package yyyq.util;

import yyyq.entity.HastenMatchingEntity;
import yyyq.util.druidOrPool.ConnectionPool;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class CsUtil {


    public static Set<String> conn() {
        HashSet<String> set = new HashSet<>();
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.15.197:3306/pb";  //  线上内网地址
        String username = "root";
        String password = "rootROOT1.";
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            String sql = "SELECT 手机号 FROM pb_tel_risk_lib  WHERE isman = 1;";
            PreparedStatement pstmt;
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String phone = rs.getString("手机号");     // 授信编号
                set.add(phone);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return set;
    }

    public static Set<String> conn_() {
        HashSet<String> set = new HashSet<>();
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.15.197:3306/pb";  //  线上内网地址
        String username = "root";
        String password = "rootROOT1.";
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            String sql = "SELECT 手机号 FROM pb_tel_risk_lib;";
            PreparedStatement pstmt;
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String phone = rs.getString("手机号");     // 授信编号
                set.add(phone);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return set;
    }

    static Connection conn;
    /*static {
        conn = getConn();
    }*/
    public CsUtil(){
        conn = getConn();
    }

    public static Connection getConn(){
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.15.198:3306/test";  //  线上内网地址
        String username = "root";
        String password = "rootROOT1.";
        try {
            Class.forName(driver);
            if (conn == null){
                conn = DriverManager.getConnection(url, username, password);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void insert(HastenMatchingEntity hasten_matching) {
        try {
            ConnectionPool connectionPool = new ConnectionPool("com.mysql.jdbc.Driver", "jdbc:mysql://192.168.15.198:3306/test", "root", "rootROOT1.");
            Connection conn = connectionPool.getConnection();
            String sql = "insert into hasten_matching (ID,phoneNum_10,callNum_10,phoneNum_20,callNum_20,phoneNum_30,callNum_30,phoneNum_90,callNum_90,phoneNum_180,callNum_180,origDate,nowDate,proportion30,top10Num,phoneNum_180_fortyThousand,phoneNum_90_fortyThousand) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1,hasten_matching.getClientNo()+"|"+hasten_matching.getCreditNo());
            pstmt.setString(2,hasten_matching.getPhoneNum_10());
            pstmt.setString(3,hasten_matching.getCallNum_10());
            pstmt.setString(4,hasten_matching.getPhoneNum_20());
            pstmt.setString(5,hasten_matching.getCallNum_20());
            pstmt.setString(6,hasten_matching.getPhoneNum_30());
            pstmt.setString(7,hasten_matching.getCallNum_30());
            pstmt.setString(8,hasten_matching.getPhoneNum_90());
            pstmt.setString(9,hasten_matching.getCallNum_90());
            pstmt.setString(10,hasten_matching.getPhoneNum_180());
            pstmt.setString(11,hasten_matching.getCallNum_180());
            pstmt.setString(12,hasten_matching.getOrigDate());
            pstmt.setString(13,hasten_matching.getNowDate());
            pstmt.setString(14,hasten_matching.getProportion30());
            pstmt.setString(15,hasten_matching.getTop10Num());
            pstmt.setString(16,hasten_matching.getPhoneNum_180_fortyThousand());
            pstmt.setString(17,hasten_matching.getPhoneNum_90_fortyThousand());
            pstmt.executeUpdate();
            connectionPool.returnConnection(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
