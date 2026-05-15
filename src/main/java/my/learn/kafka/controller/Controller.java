package my.learn.kafka.controller;

import lombok.RequiredArgsConstructor;
import my.learn.kafka.dto.InputDto;
import my.learn.kafka.dto.OutputDto;
import my.learn.kafka.service.ApiService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class Controller {

    private final ApiService service;

    @GetMapping("/test")
    public ResponseEntity<OutputDto> test(@RequestBody InputDto inputDto) {
        OutputDto dto = service.handleRequest(inputDto);
        return ResponseEntity.ok(dto);
    }

}
