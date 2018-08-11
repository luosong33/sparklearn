package yyyq.kafka.threddeam;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerSingleton.class);
    private static KafkaProducer<String, String> kafkaProducer;
//    private Random random = new Random();
    private String topic;
//    private int retry;

    private KafkaProducerSingleton() {
    }

    /**
     * 单例模式,kafkaProducer是线程安全的,可以多线程共享一个实例 
     */
    public static final KafkaProducerSingleton getInstance() {
        return new KafkaProducerSingleton();
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
                kafkaProducer = new KafkaProducer<>(props);
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