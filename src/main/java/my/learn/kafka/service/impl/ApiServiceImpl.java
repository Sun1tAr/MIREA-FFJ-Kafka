package my.learn.kafka.service.impl;

import lombok.RequiredArgsConstructor;
import my.learn.kafka.dto.FromClientTopicMessage;
import my.learn.kafka.dto.InputDto;
import my.learn.kafka.dto.OutputDto;
import my.learn.kafka.dto.ToClientTopicMessage;
import my.learn.kafka.service.ApiService;
import my.learn.kafka.service.KafkaService;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import tools.jackson.databind.util.BeanUtil;

@Service
@RequiredArgsConstructor
public class ApiServiceImpl implements ApiService {

    private final KafkaService kafkaService;

    @Override
    public OutputDto handleRequest(InputDto inputDto) {
        FromClientTopicMessage from = new FromClientTopicMessage(inputDto.getData());
        kafkaService.sendMessageFromClient(from);
        ToClientTopicMessage to = kafkaService.consumeToClientTopicMessage();
        OutputDto output = new OutputDto();
        BeanUtils.copyProperties(to, output);
        return output;
    }
}
