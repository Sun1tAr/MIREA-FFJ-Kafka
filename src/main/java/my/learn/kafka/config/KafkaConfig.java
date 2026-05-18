package my.learn.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import tools.jackson.databind.ObjectMapper;

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
