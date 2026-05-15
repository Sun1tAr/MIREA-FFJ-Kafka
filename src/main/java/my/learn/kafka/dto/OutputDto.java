package my.learn.kafka.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
public class OutputDto {

    private String data;

    @JsonProperty("internalMessage")
    private String message;

    private ZonedDateTime createdAt;

}
