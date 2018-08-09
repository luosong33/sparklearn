package yyyq.datasync.jingshuang.d_0211;

import yyyq.datasync.HastenMatchingTransferRun;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;
import yyyq.util.ReadWriteUtil;
import yyyq.util.druidOrPool.DruidUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OverdueCellDetail {

    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(50);
        ArrayList<String> list = handle("data/rpt_xjb_data.csv", "UTF-8");  //  rpt_xjb_data_test
        ArrayList<String> _list = new ArrayList<>();
        int files = 0;
        for (String s : list) {
            files++;
            _list.add(s);
            if (files % 5000 == 0) {
                ArrayList<String> list_ = new ArrayList<>();
                list_.addAll(_list);
                _list.clear();
                executor.submit(new ThreadHandle(list_));
                System.out.println(DateUtil.nowString() + " =======" + files + "=======");
            }
        }
        executor.submit(new ThreadHandle(_list));
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
                list.add(s);
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

    private static void insert(ArrayList<HashMap<String, String>> list) {
        Connection conn = null;
        try {
            conn = GetConnection.getPhoenixConn();
            conn.setAutoCommit(false);
            PreparedStatement stmt = null;
            for (HashMap<String, String> map : list) {
                String sql = "upsert into \"rpt_xjb_data\" (\"client\") values (?) ";
                conn.prepareStatement(sql);
                stmt.setString(1, map.get("client_no"));
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
            stmt.clearBatch();
            conn.commit();
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class ThreadHandle extends Thread {
        private ArrayList<String> list_ = new ArrayList<>();

        public ThreadHandle(ArrayList<String> list_) {
            this.list_ = list_;
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread() + "================Run================" + list_.size() + "================Run================");
            Connection conn = DruidUtil.getConn();
            PreparedStatement pstmt;

            for (String s : list_) {
                String sql_ = "select * from \"ods_cell_linker\" where ID like '" + s + "%'";
                try {
                    pstmt = conn.prepareStatement(sql_);
                    ResultSet rs_ = pstmt.executeQuery();
                    while (rs_.next()) {
                        String clientNo = rs_.getString("clientNo");   // 客户编号
                        String linkPhone = rs_.getString("linkPhone");   // 客户编号
                        String linker = rs_.getString("linker");   // 客户编号
                        StringBuffer sb = new StringBuffer();
                        sb.append(clientNo + ",").append(linkPhone + ",").append(linker);
                        ReadWriteUtil.write("E:\\ls\\jingshuang\\0211_1.txt", sb.toString());
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
