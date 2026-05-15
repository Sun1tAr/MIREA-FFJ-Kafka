package my.learn.kafka.service;

import my.learn.kafka.dto.FromClientTopicMessage;
import my.learn.kafka.dto.ToClientTopicMessage;

public interface KafkaService {

    void sendMessageFromClient(FromClientTopicMessage message);

    void sendMessageToClient(ToClientTopicMessage message);

    ToClientTopicMessage consumeToClientTopicMessage();

}
