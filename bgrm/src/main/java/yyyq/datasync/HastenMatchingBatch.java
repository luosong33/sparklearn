package yyyq.datasync;

import yyyq.util.DateUtil;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 单线程
 * 和two同时运行验证了mysql支持两个线程同时写入
 */
public class HastenMatchingBatch {

    //  读文件获取内容
    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();

        File file = null;
        try {
//            file = new File("E:\\ls\\0123.csv");
            file = new File("E:\\ls\\0123_1.csv");
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (file != null) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
                String tempString = "";
                int i = 0;
                ArrayList<HashMap<String, String>> list = new ArrayList<>();
                while ((tempString = reader.readLine()) != null) {
                    handle(tempString, list);
                    i++;
                    if (list.size() % 10000 == 0) {
                        updateBatch(list);
                        System.out.println("==========================" + i + "==========================");
                        list.clear();
                    }
                }

                updateBatch(list);
                System.out.println("==========================" + i + "==========================");
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " HastenMatchingTransfer 导入耗时为： " + (endtime - starttime));  //  153211  81436
    }

    public static void handle(String tempString, ArrayList<HashMap<String, String>> list) {
        String[] split = tempString.split("\\^", -1);

        String client_no = split[0].split("\\|")[0];
        String phoneNum_10 = split[1];
        String callNum_10 = split[2];
        String phoneNum_20 = split[3];
        String callNum_20 = split[4];
        String phoneNum_30 = split[5];
        String callNum_30 = split[6];
        String phoneNum_180 = split[7];
        String callNum_180 = split[8];
        String origDate = split[9];
        String nowDate = split[10];
        String phoneNum_90 = split[11];
        String callNum_90 = split[12];
        String proportion30 = split[13];
        String top10Num = split[14];
        String phoneNum_180_fortyThousand = split[15];
        String phoneNum_90_fortyThousand = split[16];
        if ("".equals(phoneNum_10)) {
            phoneNum_10 = "0";
        }
        if ("".equals(callNum_10)) {
            callNum_10 = "0";
        }
        if ("".equals(phoneNum_20)) {
            phoneNum_20 = "0";
        }
        if ("".equals(callNum_20)) {
            callNum_20 = "0";
        }
        if ("".equals(phoneNum_30)) {
            phoneNum_30 = "0";
        }
        if ("".equals(callNum_30)) {
            callNum_30 = "0";
        }
        if ("".equals(phoneNum_180)) {
            phoneNum_180 = "0";
        }
        if ("".equals(callNum_180)) {
            callNum_180 = "0";
        }
        if ("".equals(phoneNum_90)) {
            phoneNum_90 = "0";
        }
        if ("".equals(callNum_90)) {
            callNum_90 = "0";
        }
        if ("".equals(proportion30)) {
            proportion30 = "0";
        }
        if ("".equals(top10Num)) {
            top10Num = "0";
        }
        if ("".equals(phoneNum_180_fortyThousand)) {
            phoneNum_180_fortyThousand = "0";
        }
        if ("".equals(phoneNum_90_fortyThousand)) {
            phoneNum_90_fortyThousand = "0";
        }
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("client_no", client_no);
        hashMap.put("phoneNum_10", phoneNum_10);
        hashMap.put("callNum_10", callNum_10);
        hashMap.put("phoneNum_20", phoneNum_20);
        hashMap.put("callNum_20", callNum_20);
        hashMap.put("phoneNum_30", phoneNum_30);
        hashMap.put("callNum_30", callNum_30);
        hashMap.put("phoneNum_180", phoneNum_180);
        hashMap.put("callNum_180", callNum_180);
        hashMap.put("phoneNum_90", phoneNum_90);
        hashMap.put("callNum_90", callNum_90);
        hashMap.put("proportion30", proportion30);
        hashMap.put("top10Num", top10Num);
        hashMap.put("phoneNum_180_fortyThousand", phoneNum_180_fortyThousand);
        hashMap.put("phoneNum_90_fortyThousand", phoneNum_90_fortyThousand);
        list.add(hashMap);

    }

    public static void updateBatch(ArrayList<HashMap<String, String>> list) {
        Connection conn = null;
        Statement  stmt = null;
        try {
            conn = DriverManager.getConnection(
                    "jdbc:mysql://192.168.15.198:3306/test",
                    "root",
                    "rootROOT1.");
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            for (HashMap<String, String> map : list) {
                String sql = "UPDATE t_data_mx_clean SET " +
                        "MATCHED_DAN_NUMBER_10DAY = " + map.get("phoneNum_10") + ", MATCHED_DAN_FREQUENCY_10DAY = " + map.get("callNum_10") + ", " +
                        "MATCHED_DAN_NUMBER_20DAY = " + map.get("phoneNum_20") + ", MATCHED_DAN_FREQUENCY_20DAY = " + map.get("callNum_20") + ", MATCHED_DAN_NUMBER_30DAY = " + map.get("phoneNum_30") + ", " +
                        "MATCHED_DAN_FREQUENCY_30DAY = " + map.get("callNum_30") + ", MATCHED_DAN_NUMBER_90DAY = " + map.get("phoneNum_90") + ", MATCHED_DAN_FREQUENCY_90DAY = " + map.get("callNum_90") + ", " +
                        "MATCHED_DAN_NUMBER_180DAY = " + map.get("phoneNum_180") + ", MATCHED_DAN_FREQUENCY_180DAY = " + map.get("callNum_180") + ", CALL_TOP_TEN_DAN_NUMBER = " + map.get("top10Num") + ", " +
                        "DAN_NUMBER_CALL_RATE30 = " + map.get("proportion30") + ", CALL90_FORTY_THOUSAND_DAN_NUMBER = " + map.get("phoneNum_90_fortyThousand") + ", CALL180_FORTY_THOUSAND_DAN_NUMBER = " + map.get("phoneNum_180_fortyThousand") + " " +
//                        "CREATE_TIME = '" + origDate + "', UPDATE_TIME = '" + nowDate + "' " +
                        "WHERE  USER_ID = '" + map.get("client_no") + "'; ";
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
            stmt.clearBatch();
            conn.commit();
            conn.setAutoCommit(true);
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
        }
    }
}
