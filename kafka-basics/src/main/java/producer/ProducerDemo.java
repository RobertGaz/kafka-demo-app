package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //          <key type, value type>
        KafkaProducer <String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord <String, String> producerRecord = new ProducerRecord<>("java_topic", "Hello from Robert");

        //returns Future object
        producer.send(producerRecord);

        // send all data and block until done - synchronous operation
        producer.flush();

        // под капотом тоже делает flush
        producer.close();
    }
}
