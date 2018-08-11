package yyyq.datasync;

import yyyq.util.DateUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 标准多线程入库文件
 问题：多线程传递list  丢数据
 */
public class HastenMatchingTransfer {
    //  读文件获取内容
    public static void main(String[] args) {
        hastenMatchingTransfer();
    }

    private static void hastenMatchingTransfer() {
        long starttime = System.currentTimeMillis();
//        CurrencyConnPool.getInser();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        File file = null;
        try {
            file = new File("E:\\ls\\0123.csv");
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (file != null) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
                ArrayList<HashMap<String, String>> list = new ArrayList<>();
                String tempString = "";
                int i = 0;
                while ((tempString = reader.readLine()) != null) {
                    handle(tempString, list);
                    i++;
                    if (list.size() % 10000 == 0) {
                        ArrayList<HashMap<String, String>> list_ = new ArrayList<>();
                        list_.addAll(list);
                        System.out.println("================Transfer-start================"+list_.size()+"================Transfer-start================");
                        executor.submit(new HastenMatchingTransferRun(list_));
//                        new HastenMatchingTransferRun(list).run();
                        System.out.println("==========================" + i + "==========================");
                        list.clear();
                    }
                }
                executor.submit(new HastenMatchingTransferRun(list));
//                new HastenMatchingTransferRun(list).run();
                System.out.println("==========================" + i + "==========================");
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    }
                }
                executor.shutdown();
            }
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " HastenMatchingTransfer 导入耗时为： " + (endtime - starttime));
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
//        list.add(hashMap);
        synchronized(list){
            list.add(hashMap);
        }
    }

}
