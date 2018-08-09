package yyyq.kafka;

import com.alibaba.fastjson.JSON;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Kafka2PathOld extends Thread{
    //  消费者成员传参
    public String topic;
    public Kafka2PathOld(String topic) {
        this.topic = topic;
    }

    //  生产者(消费完再生产)
    public synchronized static void producer(String content, String TOPIC, String msg) {

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "SH-M1-L06-YH-node1:9092,SH-M1-L06-YH-node2:9092,SH-M1-L06-YH-node3:9092,SH-M1-L06-YH-node4:9092,SH-M1-L06-YH-node5:9092");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("max.request.size","134217728");  //  128M
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, content);
        producer.send(record, new DemoProducerCallback());
        System.out.println(msg.split(",filePath:")[0] + ")"+" | "+DateUtil.nowString());
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

    //  消费者
    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
        while(iterator.hasNext()){
            String msg = new String(iterator.next().message());
            String str = null;
            String apiCode = null;
            try {
// 样例数据     2017-10-10 15:53:51,644 - (clientNo:100120171010474630,creditNo:102020171010362377,apiCode:MOXIE_CARRIER_ACCESS_SUMMIT,
// filePath:/tmp/nfs/cdsp/moxie_carrier/basic/20171010/24fa9fb0-ad90-11e7-b29c-00163e0cb266.json) 【TimeKey=a80375f45956480198566a4b88d3c192】
                String origDate = msg.split(" - \\(")[0];
                String filePath = msg.split("filePath:")[1].split("\\)")[0];
                String clientNo = msg.split("clientNo:")[1].split(",")[0];
                String creditNo = msg.split("creditNo:")[1].split(",")[0];
                apiCode = msg.split("apiCode:")[1].split(",")[0];
                String reads = ReadWriteUtil.reads(filePath, "GBK");
                JSONObject jsonObject = JSON.parseObject(reads);
                jsonObject.put("clientNo", clientNo);
                jsonObject.put("creditNo", creditNo);
                jsonObject.put("origDate", origDate);
                str = JSON.toJSONString(jsonObject);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("数据解析错误");
            }
            producer(str, apiCode, msg);  //  +"_NEW"
        }
    }

    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.15.195:2181,192.168.15.196:2181,192.168.15.197:2181,192.168.15.198:2181,192.168.15.199:2181");//声明zk
        properties.put("group.id", "Kafka2Path_path"+"_NEW");// 生产和消费必须要使用相同的组名称， 如果生产者和消费者都不在同一组，则取不到数据
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public static void main(String[] args) throws Exception {
        new Kafka2PathOld("cdsp-dwfile-path").start();// 使用kafka集群中创建好的主题 test
    }
}

