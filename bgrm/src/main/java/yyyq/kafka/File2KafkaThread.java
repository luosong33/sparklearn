package yyyq.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yyyq.util.DateUtil;
import yyyq.util.ReadWriteUtil;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class File2KafkaThread extends Thread {

    private String message;
    public File2KafkaThread(String message) {
        this.message = message;
    }

    @Override
    public void run() {
        String[] split = message.split(",");
        String file_path = split[4];
        file_path = "D:\\tmp\\6a4fa790-8581-11e7-844a-00163e0e0050.json";
        String reads = ReadWriteUtil.reads(file_path, "GBK");
        JSONObject jobj = JSON.parseObject(reads);
        jobj.put("clientNo", split[1]);
        jobj.put("creditDate", split[2]);
        jobj.put("creditNo", split[3]);
        String str = JSON.toJSONString(jobj);

        KafkaProducer kafkaProducer = KafkaProducer.getInstance();
        kafkaProducer.init("MOXIE_CARRIER_ACCESS_SUMMIT_REPEAT");
//        System.out.println("当前线程:" + Thread.currentThread().getName() + ",获取的kafka实例:" + kafkaProducerSingleton);
        kafkaProducer.sendKafkaMessage(str);
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

        String path = "D:\\tmp\\a_history_one.txt";  //  /bigdata/ls/data/kafka/repeat/a_history_one.txt
        File file = new File(path);  //  读取清单文件
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            String tempString = null;
            int i = 0;
            while ((tempString = reader.readLine()) != null) {
                String file_path = tempString.split(",")[4];
                executor.submit(new File2KafkaThread(tempString));
                System.out.println(i++ + "==file_path==" + file_path);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                    KafkaProducer.close();
                } catch (IOException e1) {
                }
            }
        }

        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " HandlerProducer 耗时为： " + (endtime - starttime));
    }
}




class KafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(yyyq.kafka.threddeam.KafkaProducerSingleton.class);
    private static org.apache.kafka.clients.producer.KafkaProducer kafkaProducer;
    //    private Random random = new Random();
    private String topic;
//    private int retry;

    private KafkaProducer() {
    }

    /**
     * 单例模式,kafkaProducer是线程安全的,可以多线程共享一个实例
     */
    public static final KafkaProducer getInstance() {
        return new KafkaProducer();
    }

    /**
     * kafka生产者进行初始化
     */
    public void init(String topic/*,int retry*/) {
        this.topic = topic;
//        this.retry = retry;
        if (null == kafkaProducer) {
            Properties props = new Properties();
            InputStream inStream = null;
            try {
                inStream = this.getClass().getClassLoader().getResourceAsStream("kafka.properties");
                props.load(inStream);
                kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer(props);
            } catch (IOException e) {
                LOGGER.error("kafkaProducer初始化失败:" + e.getMessage(), e);
            } finally {
                if (null != inStream) {
                    try {
                        inStream.close();
                    } catch (IOException e) {
                        LOGGER.error("kafkaProducer初始化失败:" + e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * 通过kafkaProducer发送消息
     *            消息接收主题
     *            哪一个分区
     *            重试次数
     * @param message
     *            具体消息值
     */
    public synchronized void sendKafkaMessage(final String message) {
        /**
         * 1、如果指定了某个分区,会只讲消息发到这个分区上 2、如果同时指定了某个分区和key,则也会将消息发送到指定分区上,key不起作用
         * 3、如果没有指定分区和key,那么将会随机发送到topic的分区中 4、如果指定了key,那么将会以hash<key>的方式发送到分区中
         */
//        ProducerRecord<String, String> record = new ProducerRecord<>(topic, random.nextInt(3), "", message);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        // send方法是异步的,添加消息到缓存区等待发送,并立即返回，这使生产者通过批量发送消息来提高效率
        // kafka生产者是线程安全的,可以单实例发送消息
        kafkaProducer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata,
                                     Exception exception) {
                if (null != exception) {
                    LOGGER.error("kafka发送消息失败:" + exception.getMessage(),
                            exception);
//                    retryKakfaMessage(message);
                }
            }
        });
    }

    /**
     * 当kafka消息发送失败后,重试
     * @param retryMessage
     */
    /*private void retryKakfaMessage(final String retryMessage) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, random.nextInt(3), "", retryMessage );
        for (int i = 1; i <= retry; i++) {
            try {
                kafkaProducer.send(record);
                return;
            } catch (Exception e) {
                LOGGER.error("kafka重发消息失败:" + e.getMessage(), e);
                retryKakfaMessage(retryMessage);
            }
        }
    }*/

    /**
     * kafka实例销毁
     */
    public static void close() {
        if (null != kafkaProducer) {
            kafkaProducer.close();
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    /*public int getRetry() { return retry; }
    public void setRetry(int retry) {
        this.retry = retry;
    }*/

}