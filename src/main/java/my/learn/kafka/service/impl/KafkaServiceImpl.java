package my.learn.kafka.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import my.learn.kafka.dto.FromClientTopicMessage;
import my.learn.kafka.dto.ToClientTopicMessage;
import my.learn.kafka.service.KafkaService;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.ZonedDateTime;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaServiceImpl implements KafkaService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void sendMessageFromClient(FromClientTopicMessage message) {
        String s = objectMapper.writeValueAsString(message);
        kafkaTemplate.send("from-client", s);
        log.info("Message sent to topic: " + s);
    }

    @Override
    public void sendMessageToClient(ToClientTopicMessage message) {
        String s = objectMapper.writeValueAsString(message);
        kafkaTemplate.send("to-client", s);
        log.info("Message sent to topic: " + s);
    }

    @Override
    public ToClientTopicMessage consumeToClientTopicMessage() {
        return new ToClientTopicMessage(ZonedDateTime.now(),"", "");
    }
}
