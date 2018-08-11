package yyyq.kafka.cdspthread;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

public class Cdsp2KafkaProducerSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(Cdsp2KafkaProducerSingleton.class);
    private static KafkaProducer<String, String> kafkaProducer;
    private String topic;

    private Cdsp2KafkaProducerSingleton() {}

    /**
     * 单例模式,kafkaProducer是线程安全的,可以多线程共享一个实例 
     */
    public static final Cdsp2KafkaProducerSingleton getInstance() {
        return new Cdsp2KafkaProducerSingleton();
    }

    /**
     * kafka生产者进行初始化 
     */
    public void init(String topic) {
        this.topic = topic;
        if (null == kafkaProducer) {
            Properties props = new Properties();
            InputStream inStream = null;
            try {
                inStream = this.getClass().getClassLoader().getResourceAsStream("kafka.properties");
                props.load(inStream);
                kafkaProducer = new KafkaProducer<>(props);
            } catch (IOException e) {
                LOGGER.error("Cdsp2HandlerProducer初始化失败:" + e.getMessage(), e);
            } finally {
                if (null != inStream) {
                    try {
                        inStream.close();
                    } catch (IOException e) {
                        LOGGER.error("Cdsp2HandlerProducer初始化失败:" + e.getMessage(), e);
                    }
                }
            }
        }
    }

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
                    LOGGER.error("kafka发送消息失败:" + exception.getMessage(), exception);
                }
            }
        });
    }

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

}