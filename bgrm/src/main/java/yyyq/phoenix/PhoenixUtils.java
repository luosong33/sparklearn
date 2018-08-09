package yyyq.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;

public class PhoenixUtils {
	public static Connection getConnection() {
		Connection cc = null;
		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:192.168.15.195:2181/hbase";

		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		if (cc == null) {
			try {
				cc = DriverManager.getConnection(url);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return cc;
	}

	private static String[] getColNames(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		// 获取查询的列数
		int count = rsmd.getColumnCount();
		String[] colNames = new String[count];
		for (int i = 1; i <= count; i++) {
			// 获取查询类的别名
			colNames[i - 1] = rsmd.getColumnLabel(i);
		}
		return colNames;
	}

	public static Object getObject(String sql,@SuppressWarnings("rawtypes") Class clazz) throws SQLException,
			InstantiationException, IllegalAccessException,
			NoSuchMethodException, SecurityException, IllegalArgumentException,
			InvocationTargetException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			String[] colNames = getColNames(rs);

			Object object = null;
			Method[] ms = clazz.getMethods();
			if (rs.next()) {
				object = clazz.newInstance();
				for (int i = 0; i < colNames.length; i++) {
					String colName = colNames[i];
					String methodName = "set" + colName;

					for (Method md : ms) {
						if (methodName.equals(md.getName())) {
							md.invoke(object, rs.getObject(colName));
							break;
						}
					}
				}
			}
			return object;
		} finally {
			if (null != rs) {
				rs.close();
			}
			if (null != ps) {
				ps.close();
			}
			if (null != conn) {
				conn.close();
			}
		}
	}

	public static void main(String[] args) {
		PreparedStatement stmt = null;
		Connection con = getPhoenixConn();
		String sql = "select * from \"hasten_matching\" where ID='100120170706113981|102020170706017747'";
		try {
			stmt = con.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				String callNum_180 = rs.getString("callNum_180");
				System.out.println("callNum_180==============="+callNum_180);
			}
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

//		TestPhoenixEn object = getObject("", TestPhoenixEn);

		/*Configuration config = null;
		org.apache.hadoop.hbase.client.Connection connection = null;
		Table table = null;
		config = HBaseConfiguration.create();// 配置
		config.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199");// zookeeper地址
		config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		try {
			connection = ConnectionFactory.createConnection(config);
		table = connection.getTable(TableName.valueOf("hasten_matching"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		Get get = new Get(Bytes.toBytes("100120170706113981|102020170706017747"));
		Result result = null;
		try {
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("phoneNum_30:"+Bytes.toString(result.getValue(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_30"))));
		System.out.println("phoneNum_180:"+Bytes.toString(result.getValue(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_180"))));
		System.out.println("callNum_180:"+Bytes.toString(result.getValue(Bytes.toBytes("c"), Bytes.toBytes("callNum_180"))));*/
	}

	private static Connection getPhoenixConn() {
		String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:192.168.15.197:2181/hbase";
		Connection con = getConnection(url, driver);
		return con;
	}

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

}

class TestPhoenixEn{

	private String ID;
	private String phoneNum_10;
	private String callNum_10;
	private String phoneNum_20;
	private String callNum_20;
	private String phoneNum_30;
	private String callNum_30;
	private String phoneNum_180;
	private String callNum_180;

	public String getID() {
		return ID;
	}

	public void setID(String ID) {
		this.ID = ID;
	}

	public String getPhoneNum_10() {
		return phoneNum_10;
	}

	public void setPhoneNum_10(String phoneNum_10) {
		this.phoneNum_10 = phoneNum_10;
	}

	public String getCallNum_10() {
		return callNum_10;
	}

	public void setCallNum_10(String callNum_10) {
		this.callNum_10 = callNum_10;
	}

	public String getPhoneNum_20() {
		return phoneNum_20;
	}

	public void setPhoneNum_20(String phoneNum_20) {
		this.phoneNum_20 = phoneNum_20;
	}

	public String getCallNum_20() {
		return callNum_20;
	}

	public void setCallNum_20(String callNum_20) {
		this.callNum_20 = callNum_20;
	}

	public String getPhoneNum_30() {
		return phoneNum_30;
	}

	public void setPhoneNum_30(String phoneNum_30) {
		this.phoneNum_30 = phoneNum_30;
	}

	public String getCallNum_30() {
		return callNum_30;
	}

	public void setCallNum_30(String callNum_30) {
		this.callNum_30 = callNum_30;
	}

	public String getPhoneNum_180() {
		return phoneNum_180;
	}

	public void setPhoneNum_180(String phoneNum_180) {
		this.phoneNum_180 = phoneNum_180;
	}

	public String getCallNum_180() {
		return callNum_180;
	}

	public void setCallNum_180(String callNum_180) {
		this.callNum_180 = callNum_180;
	}
}