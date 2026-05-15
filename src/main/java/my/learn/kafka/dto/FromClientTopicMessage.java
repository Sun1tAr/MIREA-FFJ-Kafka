package my.learn.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
@AllArgsConstructor
public class FromClientTopicMessage {

    private final String data;

}
