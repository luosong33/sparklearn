package yyyq.kafka.threddeam;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.ReadWriteUtil;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HandlerProducer implements Runnable {

    private String message;
    public HandlerProducer(String message) {
        this.message = message;
    }

    @Override
    public void run() {
        String[] split = message.split(",");
        String file_path = split[4];
//        file_path = "D:\\tmp\\6a4fa790-8581-11e7-844a-00163e0e0050.json";
        String reads = ReadWriteUtil.reads(file_path, "GBK");
        JSONObject jobj = JSON.parseObject(reads);
        jobj.put("clientNo", split[1]);
        jobj.put("creditDate", split[2]);
        jobj.put("creditNo", split[3]);
        String str = JSON.toJSONString(jobj);

        KafkaProducerSingleton kafkaProducerSingleton = KafkaProducerSingleton.getInstance();
        kafkaProducerSingleton.init("MOXIE_CARRIER_ACCESS_SUMMIT_REPEAT");
//        System.out.println("当前线程:" + Thread.currentThread().getName() + ",获取的kafka实例:" + kafkaProducerSingleton);
        kafkaProducerSingleton.sendKafkaMessage(str);
//        kafkaProducerSingleton.sendKafkaMessage(message);
    }

    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        /*for (int i = 1; i <= 20; i++) {
            *//*try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*//*
            executor.submit(new HandlerProducer(":" + i));
        }*/

        args[0] = "D:\\tmp\\a_history_one.txt";  //  /bigdata/ls/data/kafka/repeat/a_history_one.txt
        File file = new File(args[0]);  //  读取清单文件
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            String tempString = null;
            int i = 0;
            while ((tempString = reader.readLine()) != null) {
                String file_path = tempString.split(",")[4];
                executor.submit(new HandlerProducer(tempString));
                System.out.println(i++ + "==file_path==" + file_path);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                    KafkaProducerSingleton.close();
                } catch (IOException e1) {
                }
            }
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " HandlerProducer 耗时为： " + (endtime - starttime));
    }

}