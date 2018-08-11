package yyyq.datasync.pic;

import yyyq.util.druidOrPool.DruidUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

public class PicTransferRun extends Thread {
    private ArrayList<HashMap<String, String>> list = new ArrayList<>();
    public PicTransferRun(ArrayList<HashMap<String, String>> list) {
        this.list = list;
    }

    @Override
    public void run() {
        long starttime = System.currentTimeMillis();

        Connection conn = null;
        Statement stmt = null;
        try {
            System.out.println(Thread.currentThread()+"================Run================" + list.size() + "================Run================");
            conn = DruidUtil.getConn();                 //  测试通过，连接池10个，开源组件健壮，可自动连接等待，推荐使用
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            for (HashMap<String, String> map : list) {
                String sql = "INSERT into pic_scoring (pic_path,ave_fraction) VALUES (\"" +
                        map.get("pic_src") + "\", \"" + map.get("pic_score") + "\") ";
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
//            connectionPool.returnConnection(conn);
//            System.out.println();
        }

        long endtime = System.currentTimeMillis();
        System.out.println(" PicTransferRun 导入耗时为： " + (endtime - starttime));
    }

}
