package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import producer.ProducerDemo;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "my-java-app");

        //possible values here: auto/earliest/latest.
        // если none, то такая consumer group уже должна существовать
        // earliest - консумер начнет читать с самого раннего рекорда (как --from-beginning)
        // latest - консумер начнет читать с рекордов, отправленных после его запуска
        properties.setProperty("auto.offset.reset", "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Collections.singletonList("java_topic"));

        while (true) {
            LOGGER.info("polling...");
                                                                   //сколько максимум ждать
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("key: " + record.key() + ", value: " + record.value()
                        + ", partition: " + record.partition() + ", offset: " + record.offset());
            }
        }
    }
}
