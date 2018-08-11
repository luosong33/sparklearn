package yyyq.datasync.pic;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 标准多线程入库文件
 */
public class PicTransfer {
    //  读文件获取内容
    public static void main(String[] args) {
        hastenMatchingTransfer();
    }

    private static void hastenMatchingTransfer() {
        long starttime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        File file = null;
        try {
            file = new File("data/picdata.json");
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
                    if (list.size() % 1000 == 0) {
                        ArrayList<HashMap<String, String>> list_ = new ArrayList<>();
                        list_.addAll(list);
                        System.out.println("================Transfer-start================" + list_.size() + "================Transfer-start================");
                        executor.submit(new PicTransferRun(list_));
                        System.out.println("==========================" + i + "==========================");
                        list.clear();
                    }
                }
                executor.submit(new PicTransferRun(list));
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
        System.out.println(" PicTransfer 导入耗时为： " + (endtime - starttime));
    }

    public static void handle(String tempString, ArrayList<HashMap<String, String>> list) {
        JSONObject parse = (JSONObject) JSON.parse(tempString);

        String pic_src = parse.getString("pic_src");
        String pic_score = parse.getString("pic_score");
        String[] split = pic_src.split("/", -1);
        pic_src = split[split.length - 1];
        if ("".equals(pic_src)) {
            pic_src = "0";
        }
        if ("".equals(pic_score)) {
            pic_score = "0";
        }
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("pic_src", pic_src);
        hashMap.put("pic_score", pic_score);
//        list.add(hashMap);
        synchronized (list) {
            list.add(hashMap);
        }
    }

}
