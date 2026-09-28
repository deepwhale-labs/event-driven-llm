package io.github.deepwhalelabs.eventdrivenllm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CoralResultConsumer {
    private static final Logger log = LoggerFactory.getLogger(CoralResultConsumer.class);
    private final CoralClient coral;
    private final ObjectMapper mapper;

    public CoralResultConsumer(CoralClient coral, ObjectMapper mapper) {
        this.coral = coral;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "${app.kafka.result-topic}", groupId = "coral-notifier")
    public void consume(String payload) throws Exception {
        LlmResult result = mapper.readValue(payload, LlmResult.class);
        coral.deliver(result);
        log.info("Delivered task {} to Coral", result.taskId());
    }
}
