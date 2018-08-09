package yyyq.datasync.jingshuang.d_0211;

import yyyq.util.DateUtil;
import yyyq.util.HBaseUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CountTest {

    public static void main(String[] args) throws Exception {
        long starttime = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(10);
        String path = "data/000000_0";
        StringBuffer sb = new StringBuffer();
        try {
            FileInputStream inputStream = new FileInputStream(path);
            //  字节流中指定编码
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String s = null;
            ArrayList<Map<String, String>> list = new ArrayList<>();
            while ((s = bufferedReader.readLine()) != null) {
                //  逻辑
                HashMap<String, String> hashMap = new HashMap<>();

                String[] split = s.split(",", -1);
                try {
                    hashMap.put("rowKey", split[3]+"_"+split[1]);
                    hashMap.put("linkPhone", "13333333333");
                    hashMap.put("linker", split[2]);
                    hashMap.put("clientNo", split[3]);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(s);
                }
                list.add(hashMap);
                if (list.size() % 1000 == 0) {
                    ArrayList<Map<String, String>> list_ = new ArrayList<>();
                    list_.addAll(list);
                    executor.submit(new ThreadInsert(list_));
                    list.clear();
                }
            }
            executor.submit(new OverdueInsert.ThreadInsert(list));
        } catch (IOException e) {
            e.printStackTrace();
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " CountTest 耗时为： " + (endtime - starttime));
    }

    static class ThreadInsert extends Thread {
        private List<Map<String, String>> list_ = new ArrayList<>();
        public ThreadInsert(List<Map<String, String>> list_) {
            this.list_ = list_;
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread() + "================Run================" + list_.size() + "================Run================");
            HBaseUtils.putBach_("ods_cell_linker_test", list_);
        }
    }

}
