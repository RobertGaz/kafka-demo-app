package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("batch.size", "200");

        //          <key type, value type>
        KafkaProducer <String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 500; i++) {
            ProducerRecord <String, String> producerRecord = new ProducerRecord<>("java_topic", "Hello from Robert number " + i);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // invoked every time record sent or exception happened
                    if (e == null) {
                        LOGGER.info("Received Metadata\n " +
                                "topic: " + metadata.topic() + "\n" +
                                "partition: " + metadata.partition() + "\n" +
                                "offset: " + metadata.offset() + "\n" +
                                "timestamp: " + metadata.timestamp());
                    } else {
                        LOGGER.error("Exception happened: ", e);
                    }
                }
            });

        }
        // notice, that all 50 records will go to the same partition
        // this is because we didn't specify partitioner and default is StickyPartitioner which sends records in a batch - optimization
        // You may set batch.size to 200 (these are bytes), so batches will become smaller and you'll see that they go to different partitions


        LOGGER.info("Going to flush and close.");

        // send all data and block until done - synchronous operation
        producer.flush();
        
        // под капотом тоже делает flush
        producer.close();
    }
}
