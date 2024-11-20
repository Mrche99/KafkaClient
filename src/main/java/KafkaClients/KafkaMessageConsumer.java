package KafkaClients;

import KafkaSerializer.DeserializerForKafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.example.MyMessage;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaMessageConsumer {
    private final KafkaConsumer<String, String> consumer;
    public KafkaMessageConsumer(String bootstrapServers, String topic)
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", DeserializerForKafka.class.getName());
        props.put("auto.offset.reset", "latest");
        props.put("targetClass", MyMessage.class.getName());

        this.consumer = new KafkaConsumer<>(props);

    }
    public void consumeLastMessage(String topic, int numMessages,int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPartition));

        // Устанавливаем смещение на конец партиции
        consumer.seekToEnd(Collections.singletonList(topicPartition));
        long endOffset = consumer.position(topicPartition);

        // Рассчитываем начальное смещение
        long startOffset = Math.max(endOffset - numMessages, 0);
        consumer.seek(topicPartition, startOffset);

        // Читаем сообщения
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        if (records.isEmpty()) {
            System.out.println("Нет новых сообщений в указанном диапазоне смещений.");
        } else {
            System.out.println("=== Полученные сообщения ===");
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf(
                        "Топик: %s | Раздел: %d | Смещение: %d%nКлюч: %s%nСообщение: %s%n--------------------%n",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value()
                );
            }
        }
    }

    public void close(){
        consumer.close();

}
}

