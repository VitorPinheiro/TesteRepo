package pucrio.br.lac.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

public class ConsumerCreator {
    public static KafkaConsumer<Integer, String> createConsumer(List<String> topic, String brokerAddress, String clientID, Integer maxPollRecords, String offsetResetEarlier)
    {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress); // IKafkaConstants.KAFKA_BROKERS
        props.put(ConsumerConfig.GROUP_ID_CONFIG, clientID); // IKafkaConstants.GROUP_ID_CONFIG
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords); // IKafkaConstants.MAX_POLL_RECORDS
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetEarlier); // IKafkaConstants.OFFSET_RESET_EARLIER

        final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(topic);
        return consumer;
    }
}
