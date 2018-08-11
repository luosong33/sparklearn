package yyyq.kafka.threddeam;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * 多消费者,多个work线程,难保证分区消息消费的顺序性
 * @author tanjie
 */
public final class ConsumerThread implements Runnable {

    private ConsumerRecords<String, String> records;
    private KafkaConsumer<String, String> consumer;

    public ConsumerThread(ConsumerRecords<String, String> records,
                          KafkaConsumer<String, String> consumer) {
        this.records = records;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records
                    .records(partition);
            int i = 0;
            for (ConsumerRecord<String, String> record : partitionRecords) {
                /*System.out.println("当前线程:" + Thread.currentThread() + ","
                        + "偏移量:" + record.offset() + ","
                        + "主题:" + record.topic() + ","
                        + "分区:" + record.partition() + ","
                        + "获取的消息:" + record.value());*/
                i++;
            }
            System.out.println("===i==="+i);
            // 消费者自己手动提交消费的offest,确保消息正确处理后再提交
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1)
                    .offset();
            consumer.commitSync(Collections.singletonMap(partition,
                    new OffsetAndMetadata(lastOffset + 1)));
        }
    }

    public static void main(String[] args) {
        Kafka_Consumer kafka_Consumer = new Kafka_Consumer();
        try {
            kafka_Consumer.execute();
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafka_Consumer.shutdown();
        }
    }
}