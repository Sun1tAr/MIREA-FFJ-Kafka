package my.learn.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConfig {

    @Bean
    public NewTopic fromClientTopic(@Value("${spring.kafka.topics.from-client.name}") String name,
                              @Value("${spring.kafka.topics.from-client.partitions}") int partitions,
                              @Value("${spring.kafka.topics.from-client.replicas}") int replicas,
                              @Value("${spring.kafka.topics.from-client.retention-ms}") String retentionMs,
                              @Value("${spring.kafka.topics.from-client.retention-bytes}") String retentionBytes) {
        return createTopic(name, partitions, replicas, retentionMs, retentionBytes);
    }

    @Bean
    public NewTopic toClientTopic(@Value("${spring.kafka.topics.to-client.name}") String name,
                                    @Value("${spring.kafka.topics.to-client.partitions}") int partitions,
                                    @Value("${spring.kafka.topics.to-client.replicas}") int replicas,
                                    @Value("${spring.kafka.topics.to-client.retention-ms}") String retentionMs,
                                    @Value("${spring.kafka.topics.to-client.retention-bytes}") String retentionBytes) {
        return createTopic(name, partitions, replicas, retentionMs, retentionBytes);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Value("${spring.kafka.consumer.group-id}")
    private String kafkaConsumerGroupId;
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.producer.client-id}")
    private String kafkaProducerId;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String kafkaConsumerAutoOffsetReset;
    @Value("${spring.kafka.consumer.partition-assignment}")
    private String kafkaConsumerPartitionAssignment;
    @Value("${spring.kafka.producer.enable-idempotence}")
    private Boolean kafkaProducerEnableIdempotence;
    @Value("${spring.kafka.producer.max-in-flight-requests}")
    private String kafkaProducerMaxInFlightsRequests;
    @Value("${spring.kafka.producer.acks}")
    private String kafkaProducerAcks;



    public ConsumerFactory<Integer, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,kafkaConsumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,kafkaConsumerAutoOffsetReset);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,kafkaConsumerPartitionAssignment);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    public Map<String, Object> producerConfigs() {

        log.info("Будстрап сервер конфиг: [{}]",bootstrapServers);

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProducerId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, kafkaProducerEnableIdempotence);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, kafkaProducerMaxInFlightsRequests);
        props.put(ProducerConfig.ACKS_CONFIG, kafkaProducerAcks);

        return props;
    }

    @Bean
    public ProducerFactory<Long, String> producerMessageFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<Long, String> kafkaTemplate() {
        KafkaTemplate<Long, String> template = new KafkaTemplate<>(producerMessageFactory());
        template.setMessageConverter(new MessagingMessageConverter());

        LoggingProducerListener<Long, String> listener =  new LoggingProducerListener<>();
        template.setProducerListener(listener);
        return template;
    }



    private NewTopic createTopic(String name,
                          int partitions,
                          int replicas,
                          String retentionMs,
                          String retentionBytes) {
        log.info("Создание топика с именем [{}]", name);
        return TopicBuilder
                .name(name)
                .partitions(partitions)
                .replicas(replicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, retentionMs)
                .config(TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes)
                .build();
    }

}
