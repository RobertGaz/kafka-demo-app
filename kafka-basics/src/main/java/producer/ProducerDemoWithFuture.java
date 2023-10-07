package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerDemoWithFuture {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithFuture.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //          <key type, value type>
        KafkaProducer <String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord <String, String> producerRecord = new ProducerRecord<>("java_topic", "Hello from Robert");

        //returns Future object
        Future<RecordMetadata> future = producer.send(producerRecord);

        RecordMetadata metadata = null;
        try {
            metadata = future.get();
        } catch (Exception e) {
            System.out.println("LOL");
        }
        LOGGER.info("Received Metadata\n " +
                "topic: " + metadata.topic() + "\n" +
                "partition: " + metadata.partition() + "\n" +
                "offset: " + metadata.offset() + "\n" +
                "timestamp: " + metadata.timestamp());

        // send all data and block until done - synchronous operation
//        producer.flush();

        // под капотом тоже делает flush
        producer.close();
    }
}
