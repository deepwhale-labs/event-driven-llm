package io.github.deepwhalelabs.eventdrivenllm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CoralResultConsumer {
    public static final String GROUP_ID = "coral-notifier";
    private static final Logger log = LoggerFactory.getLogger(CoralResultConsumer.class);
    private final CoralClient coral;
    private final ObjectMapper mapper;
    private final TaskStore tasks;

    public CoralResultConsumer(CoralClient coral, ObjectMapper mapper, TaskStore tasks) {
        this.coral = coral;
        this.mapper = mapper;
        this.tasks = tasks;
    }

    @KafkaListener(topics = "${app.kafka.result-topic}", groupId = GROUP_ID)
    public void consume(String payload) throws Exception {
        LlmResult result = mapper.readValue(payload, LlmResult.class);
        tasks.deliver(result, coral);
        log.info("Delivered task {} to Coral", result.taskId());
    }
}
