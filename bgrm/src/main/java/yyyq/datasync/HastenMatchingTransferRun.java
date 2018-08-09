package yyyq.datasync;

import yyyq.util.DateUtil;
import yyyq.util.druidOrPool.ConnectionPool;
import yyyq.util.druidOrPool.CurrencyConnPool;
import yyyq.util.druidOrPool.DruidUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

/**
* 标准多线程入库文件
 */
public class HastenMatchingTransferRun extends Thread {
    private ArrayList<HashMap<String, String>> list = new ArrayList<>();


    public HastenMatchingTransferRun(ArrayList<HashMap<String, String>> list) {
        this.list = list;
    }

    @Override
    public void run() {
        long starttime = System.currentTimeMillis();

        Connection conn = null;
        Statement stmt = null;
        ConnectionPool connectionPool = new ConnectionPool("com.mysql.jdbc.Driver", "jdbc:mysql://192.168.15.198:3306/test", "root", "rootROOT1.");
        try {
            System.out.println(Thread.currentThread()+"================Run================" + list.size() + "================Run================");
            conn = DruidUtil.getConn();                 //  测试通过，连接池10个，开源组件健壮，可自动连接等待，推荐使用
//            conn = connectionPool.getConnection();    //  测试通过，自写完整mysql连接池
            /*CurrencyConnPool.getInser();
            conn = CurrencyConnPool.getConnection();    //  测试通过，自写简易mysql连接池，线程池必须大于等于使用数；用多线程入库，51次批量入库，连接池连接数需大于等于51*/
            /*conn = DriverManager.getConnection(       //  测试通过，50秒左右
                    "jdbc:mysql://192.168.15.198:3306/test",
                    "root",
                    "rootROOT1.");*/
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            for (HashMap<String, String> map : list) {
                String sql = "UPDATE t_data_mx_clean SET " +
                        "MATCHED_DAN_NUMBER_10DAY = " + map.get("phoneNum_10") + ", MATCHED_DAN_FREQUENCY_10DAY = " + map.get("callNum_10") + ", " +
                        "MATCHED_DAN_NUMBER_20DAY = " + map.get("phoneNum_20") + ", MATCHED_DAN_FREQUENCY_20DAY = " + map.get("callNum_20") + ", MATCHED_DAN_NUMBER_30DAY = " + map.get("phoneNum_30") + ", " +
                        "MATCHED_DAN_FREQUENCY_30DAY = " + map.get("callNum_30") + ", MATCHED_DAN_NUMBER_90DAY = " + map.get("phoneNum_90") + ", MATCHED_DAN_FREQUENCY_90DAY = " + map.get("callNum_90") + ", " +
                        "MATCHED_DAN_NUMBER_180DAY = " + map.get("phoneNum_180") + ", MATCHED_DAN_FREQUENCY_180DAY = " + map.get("callNum_180") + ", CALL_TOP_TEN_DAN_NUMBER = " + map.get("top10Num") + ", " +
                        "DAN_NUMBER_CALL_RATE30 = " + map.get("proportion30") + ", CALL90_FORTY_THOUSAND_DAN_NUMBER = " + map.get("phoneNum_90_fortyThousand") + ", CALL180_FORTY_THOUSAND_DAN_NUMBER = " + map.get("phoneNum_180_fortyThousand") + " " +
                        "WHERE  USER_ID = '" + map.get("client_no") + "'; ";
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
            stmt.clearBatch();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                conn.rollback();
                conn.setAutoCommit(true);
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
//            CurrencyConnPool.returnConn(conn);
            connectionPool.returnConnection(conn);
//            System.out.println();
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " HastenMatchingTransferRun 导入耗时为： " + (endtime - starttime));
    }

}
