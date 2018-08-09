package yyyq.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import yyyq.util.DateUtil;
import yyyq.util.ReadWriteUtil;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class File2KafkaOld extends Thread {

    //  生产者(消费完再生产)
    public static void producer (String TOPIC, String content) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "SH-M1-L06-YH-node1:9092,SH-M1-L06-YH-node2:9092,SH-M1-L06-YH-node3:9092,SH-M1-L06-YH-node4:9092,SH-M1-L06-YH-node5:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("max.request.size", "134217728");
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, content);
        producer.send(record, new DemoProducerCallback());
        producer.close();
    }

    private static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long starttime = System.currentTimeMillis();
        File file = new File("/bigdata/ls/user_history.txt");  //  读取清单文件
//        File file = new File("D:\\tmp\\user_history_10000.txt");  //  读取清单文件
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            String tempString = null;
            int i = 0;
            while ((tempString = reader.readLine()) != null) {
                String[] split = tempString.split(",");
                String client_no = split[0];
                String apply_date = split[2];
                String VOUCHER_NO = split[3];
                String file_path = split[4];
//                file_path = "D:\\tmp\\6a4fa790-8581-11e7-844a-00163e0e0050.json";
                String reads = ReadWriteUtil.reads(file_path, "GBK");
                JSONObject jobj = JSON.parseObject(reads);
                jobj.put("client_no",client_no);
                jobj.put("apply_date",apply_date);
                jobj.put("VOUCHER_NO",VOUCHER_NO);
                String str = JSON.toJSONString(jobj);

                producer("test_thread", str);
                System.out.println(i+++"==file_path=="+file_path);
            }
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
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " File2KafkaOld 耗时为： " + (endtime - starttime));
    }
}

