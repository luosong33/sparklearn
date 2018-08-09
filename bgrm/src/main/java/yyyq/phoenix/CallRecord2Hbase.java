package yyyq.phoenix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.GetConnection;

public class CallRecord2Hbase {

    private static Connection getPhoenixConn() {
        String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
        String url = "jdbc:phoenix:192.168.15.195:2181/hbase";
        Connection con = GetConnection.getConnection(url, driver);
        return con;
    }

    //  接收数组入库
    public static void insertIntoHbase() {
        PreparedStatement stmt = null;
        Connection con = getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("");
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        try {
            stmt.addBatch();
            stmt.executeBatch();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        insertIntoHbase();
        long endtime = System.currentTimeMillis();
        System.out.println("耗时为： " + (endtime - starttime));
    }
}