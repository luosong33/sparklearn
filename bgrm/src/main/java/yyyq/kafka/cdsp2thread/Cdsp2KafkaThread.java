package yyyq.kafka.cdsp2thread;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import yyyq.kafka.cdspthread.Cdsp2HandlerProducer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Cdsp2KafkaThread extends Thread{
    //  消费者成员传参
    public String topic;
    public ExecutorService executor;

    public Cdsp2KafkaThread(String topic) {
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
        ExecutorService executorConsumer = Executors.newFixedThreadPool(10);
        executorConsumer.submit(new Cdsp2KafkaThread("cdsp-dwfile-path-history"));
    }
}

