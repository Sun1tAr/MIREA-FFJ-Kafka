package my.learn.kafka.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import my.learn.kafka.dto.FromClientTopicMessage;
import my.learn.kafka.dto.ToClientTopicMessage;
import my.learn.kafka.service.KafkaService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.ZonedDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class Listener {

    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "from-client")
    public void listen(String message){
        log.info("Received From Client Message: {}", message);
        FromClientTopicMessage object = objectMapper.readValue(message, FromClientTopicMessage.class);

        ToClientTopicMessage msg = new ToClientTopicMessage(
                ZonedDateTime.now(),
                "Сообщение от слушателя",
                object.getData()
        );

        kafkaService.sendMessageToClient(msg);
    }

}
