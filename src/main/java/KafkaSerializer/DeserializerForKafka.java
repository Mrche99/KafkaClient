package KafkaSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


public class DeserializerForKafka<T> implements Deserializer<T> {

    private final ObjectMapper mapper = new ObjectMapper();
    private Class<T> targetClass;

    public DeserializerForKafka() {
        // Конструктор по умолчанию
    }
    //Метод для конфигурации десериализатора
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            String className = (String) configs.get("targetClass");//Получение имя класса из конфигурации
            this.targetClass = (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Не удалось загрузить targetClass", e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.readValue(data, targetClass);//Десериализуем JSON из байтового массива в объект класса targetClass
        } catch (Exception e) {
            throw new RuntimeException("Ошибка десериализации", e);
        }
    }
    @Override
    public void close() {

    }
}
