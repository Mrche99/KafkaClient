package org.example;

import KafkaClients.KafkaMessageConsumer;
import KafkaClients.KafkaMessageProducer;

public class Main {
    public static void main(String[] args) {

        String bootstrapServers = "localhost:9092";
        String topic = "testTopic";

        KafkaMessageProducer producer = new KafkaMessageProducer(bootstrapServers);
        try {
            producer.sendMessage(topic, new MyMessage("test5", 332321));
            producer.close();
            System.out.println("Сообщение отправлено");
        } catch (Exception e) {
            throw new RuntimeException("Произошла ошибка при отправке сообщения");
        }


        KafkaMessageConsumer consumer = new KafkaMessageConsumer(bootstrapServers, topic);
        try {
            consumer.consumeLastMessage(topic, 5,0);
            consumer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


