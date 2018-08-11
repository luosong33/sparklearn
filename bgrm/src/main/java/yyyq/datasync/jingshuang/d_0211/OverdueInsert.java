package yyyq.datasync.jingshuang.d_0211;

import yyyq.util.DateUtil;
import yyyq.util.HBaseUtils;
import yyyq.util.ReadWriteUtil;
import yyyq.util.druidOrPool.DruidUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/* 多线程hbase API 入库 将所有逾期用户的通讯录加载到phoenix备用 */
public class OverdueInsert {

    public static void main(String[] args) throws Exception {
        long starttime = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(10);
        String path = "E:\\ls\\jingshuang\\0211.txt";
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
                    hashMap.put("rowKey", split[0]+"_"+split[1]);
                    hashMap.put("called_phone", split[1]);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(s);
                }
                list.add(hashMap);
                if (list.size() % 100000 == 0) {
                    ArrayList<Map<String, String>> list_ = new ArrayList<>();
                    list_.addAll(list);
                    executor.submit(new ThreadInsert(list_));
                    list.clear();
                }
            }
            executor.submit(new ThreadInsert(list));
        } catch (IOException e) {
            e.printStackTrace();
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " YyydInfoUser 耗时为： " + (endtime - starttime));
    }

    static class ThreadInsert extends Thread {
        private List<Map<String, String>> list_ = new ArrayList<>();

        public ThreadInsert(List<Map<String, String>> list_) {
            this.list_ = list_;
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread() + "================Run================" + list_.size() + "================Run================");
            HBaseUtils.putBach_("rpt_xjb_data_test", list_);
        }
    }

}
