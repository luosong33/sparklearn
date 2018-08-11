package yyyq.datasync.xiaobo.d_0302;

import yyyq.util.DateUtil;
import yyyq.util.HBaseUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Inserter_0302 {

    public static void main(String[] args) throws Exception {
        long starttime = System.currentTimeMillis();

        String path = "data/0302.txt";
        StringBuffer sb = new StringBuffer();
        try {
            FileInputStream inputStream = new FileInputStream(path);
            //  字节流中指定编码
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String s = null;
            ArrayList<Map<String, String>> list = new ArrayList<>();
            int i = 0;
            while ((s = bufferedReader.readLine()) != null) {
                HashMap<String, String> hashMap = new HashMap<>();

                hashMap.put("rowKey", i++ + "");
                hashMap.put("called_phone", s);
                list.add(hashMap);
                if (list.size() % 1000 == 0) {
                    ArrayList<Map<String, String>> list_ = new ArrayList<>();
                    list_.addAll(list);
                    HBaseUtils.putBach_("xiaoboi_0302", list_);
                    list.clear();
                }
            }
            HBaseUtils.putBach_("xiaoboi_0302", list);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " Inserter_0302 耗时为： " + (endtime - starttime));
    }


}
