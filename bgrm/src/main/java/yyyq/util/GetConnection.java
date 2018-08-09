package yyyq.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class GetConnection {
	public static Connection getConnection(String url,String driver){
		Connection con = null;
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		if (con == null) {
			try {
				con = DriverManager.getConnection(url);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return con;
	}

	public static Connection getPhoenixConn() {
		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:192.168.15.195:2181/hbase";
		Connection con = GetConnection.getConnection(url, driver);
		return con;
	}

	public static Connection getConn_Cdsp_Riskcore() {
		String driver = "com.mysql.jdbc.Driver";
//		String url = "jdbc:mysql://122.144.139.60:3325/cdsp";
//		String url = "jdbc:mysql://192.168.50.2:3306/cdsp";  //  线上内网地址
		String url = "jdbc:mysql://192.168.50.180:3306/cdsp";  //  线上内网地址
		String username = "linshi";
		String password = "linshi@0808";
		Connection conn = null;
		try {
			Class.forName(driver); // classLoader,加载对应驱动
			conn = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static Connection getConn_Bops_Sales() {
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://192.168.2.25:3306";  //  线上内网地址
		String username = "linshi";
		String password = "linshi@0808";
		Connection conn = null;
		try {
			Class.forName(driver); // classLoader,加载对应驱动
			conn = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
}
