package my.learn.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
@AllArgsConstructor
public class ToClientTopicMessage {

    private final ZonedDateTime createdAt;
    private final String message;
    private final String data;

}
