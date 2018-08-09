package cn.jstorm;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 1、写一个kafka消费者拉取生产者发送的数据，并继承BaseRichSpout。拉取数据后发送给Bolt
 */
public class WordReader extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private Logger log = LoggerFactory.getLogger(WordReader.class);
    private SpoutOutputCollector collector;
    private KafkaConsumer<String, String> consumer;
    public void ack(Object msgId) {
        log.info("OK:{}",msgId);
    }
    public void close() {
        consumer.close();
    }
    public void fail(Object msgId) {
        log.info("FAIL:{}",msgId);
    }

    /**
     * We will create the file and get the collector object
     * 这是第一个方法，里面接收了三个参数，第一个是创建Topology时的配置，
     * 第二个是所有的Topology数据，第三个是用来把Spout的数据发射给bolt 
     */
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        String bootstrapServers = conf.get("bootstrapServers").toString();
        String topic = conf.get("topic").toString();
        String groupId = conf.get("groupId").toString();
        //初始化kafka消费者
        initKafkaConsumer(bootstrapServers,topic,groupId);
        //初始化发射器 
        this.collector = collector;
    }

    /**
     * The only thing that the methods will do It is emit each 
     * file line
     * 
     * 这是Spout最主要的方法，在这里我们读取文本文件，并把它的每一行发射出去（给bolt） 
     * 这个方法会不断被调用，为了降低它对CPU的消耗，当任务完成时让它sleep一下 
     * 
     * nextTuple会在同一个循环内被ack()和fail()周期性的调用。没有任务时它必须释放对线程的控制，
     * 其它方法才有机会得以执行。因此nextTuple的第一行就要检查是否已处理完成。
     * 如果完成了，为了降低处理器负载，会在返回前休眠一毫秒。如果任务完成了，文件中的每一行都已被读出并分发了
     * 
     */
    public void nextTuple() {
        try{
            //消费数据
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (final ConsumerRecord<String, String> record : records) {
                String str = record.value();
                log.info("============consumer str:{},partition:{},key:{}=============",str,record.partition(),record.key());
                this.collector.emit(new Values(str),str);
            }
        }catch(Exception e){
            throw new RuntimeException("Error consumer tuple",e);
        }
    }

    /**
     * Declare the output field "line"
     * 声明输入域"line"
     */
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("line"));
//    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }


    public void initKafkaConsumer(String bootstrapServers,String topic,String groupId){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }
}