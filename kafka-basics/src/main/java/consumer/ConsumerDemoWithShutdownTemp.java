package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import producer.ProducerDemo;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdownTemp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "one_partition_topic_group_3");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("enable.auto.commit", "true");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("Detected a shutdown!!");
                consumer.wakeup(); // next time we do consumer.poll() it will throw wakeup exception

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        consumer.subscribe(Collections.singletonList("one_partition_topic"));

        while (true) {
            try {
                //сколько максимум ждать
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("key: " + record.key() + ", value: " + record.value()
                            + ", partition: " + record.partition() + ", offset: " + record.offset());
                }
            } catch (WakeupException e) {
                LOGGER.info("Shutting down the consumer...");
                consumer.close(); // this will also commit the offsets
                LOGGER.info("Shut down successful.");
                return;
            }
        }
    }
}
