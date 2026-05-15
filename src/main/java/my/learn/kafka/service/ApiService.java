package my.learn.kafka.service;

import my.learn.kafka.dto.InputDto;
import my.learn.kafka.dto.OutputDto;

public interface ApiService {

    OutputDto handleRequest(InputDto inputDto);

}
