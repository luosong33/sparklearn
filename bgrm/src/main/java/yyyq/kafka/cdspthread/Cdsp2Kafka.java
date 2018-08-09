package yyyq.kafka.cdspthread;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Cdsp2Kafka extends Thread{
    //  消费者成员传参
    public String topic;
    public ExecutorService executor;

    public Cdsp2Kafka(String topic) {
        this.topic = topic;
        this.executor = Executors.newFixedThreadPool(10);
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
//            producer(str, apiCode, msg);
            executor.submit(new Cdsp2HandlerProducer(msg));
        }
    }

    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.15.195:2181,192.168.15.196:2181,192.168.15.197:2181,192.168.15.198:2181,192.168.15.199:2181");//声明zk
        properties.put("group.id", "cdsp2kafka_consumer_history");// 生产和消费必须要使用相同的组名称， 如果生产者和消费者都不在同一组，则取不到数据
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public static void main(String[] args) throws Exception {
        new Cdsp2Kafka("cdsp-dwfile-path-history").start();// 使用kafka集群中创建好的主题 cdsp-dwfile-path-history     cdsp-dwfile-path
    }
}

