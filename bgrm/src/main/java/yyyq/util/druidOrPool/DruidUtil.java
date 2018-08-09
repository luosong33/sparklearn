package yyyq.util.druidOrPool;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledPreparedStatement;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 失败
 */
public class DruidUtil {

    //  静态内部类，既实现了线程安全，又避免了同步带来的性能影响
    private static class Inner {
        public static DruidDataSource dataSource = null;

        static {
            if (dataSource == null) {
                //创建了一个实例
                Properties properties = new Properties();
                InputStream is = DruidUtil.class.getClassLoader().getResourceAsStream("properties/dbconfig.properties");
                try {
                    properties.load(is);
                    dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static Connection getConn() {
        try {
            return Inner.dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        //        DruidPooledConnection conn = (DruidPooledConnection) DruidUtil.getConn();
        Connection conn = DruidUtil.getConn();
        String sql = "SELECT COUNT(*) FROM t_data_mx_clean WHERE MATCHED_DAN_FREQUENCY_90DAY != '';";
//        DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) conn.prepareStatement(sql);
        DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            String mobile_answer = rs.getString("COUNT(*)");
            System.out.println(mobile_answer);
        }
    }

}