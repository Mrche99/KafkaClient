package KafkaClients;

import KafkaSerializer.SerializerForKafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.example.MyMessage;

import java.util.Properties;


public class KafkaMessageProducer {
    private final Producer<String, MyMessage> producer;
    public KafkaMessageProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", SerializerForKafka.class.getName());

        this.producer = new KafkaProducer<>(props);
    }
    public void sendMessage(String topic, MyMessage message) {
        producer.send(new ProducerRecord<>(topic, message));
    }
    public void close() {
        producer.close();
    }

}

