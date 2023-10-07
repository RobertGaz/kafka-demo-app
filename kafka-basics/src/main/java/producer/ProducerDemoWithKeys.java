package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //          <key type, value type>
        KafkaProducer <String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 30; i++) {
            String topic = "java_topic";
            String key   = "id_" + i%5;
            String value = "Hello from Robert number " + i;

            ProducerRecord <String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // invoked every time record sent or exception happened
                    if (e == null) {
                        LOGGER.info("key: " + key + " | partition: " + metadata.partition());
                    } else {
                        LOGGER.error("Exception happened: ", e);
                    }
                }
            });

        }

        LOGGER.info("Going to flush and close.");

        // send all data and block until done - synchronous operation
        producer.flush();
        
        // под капотом тоже делает flush
        producer.close();
    }
}
