package KafkaSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;




public class SerializerForKafka<T> implements Serializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
          try {
            return objectMapper.writeValueAsBytes(data);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void close() {

    }
}
