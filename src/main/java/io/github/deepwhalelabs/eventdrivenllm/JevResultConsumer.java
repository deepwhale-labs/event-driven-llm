package io.github.deepwhalelabs.eventdrivenllm;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "app.jev.mode", havingValue = "shadow")
public class JevResultConsumer {
    public static final String GROUP_ID = "jev-evaluator";
    private final ObjectMapper mapper;
    private final EvaluationStore evaluations;
    public JevResultConsumer(ObjectMapper mapper, EvaluationStore evaluations) { this.mapper = mapper; this.evaluations = evaluations; }

    // Persistence failure is retried without changing the independent delivery task's state.
    @KafkaListener(topics = "${app.kafka.result-topic}", groupId = GROUP_ID, containerFactory = "jevKafkaListenerContainerFactory",
            properties = "auto.offset.reset=latest")
    public void consume(String payload) throws Exception {
        LlmResult event = mapper.readValue(payload, LlmResult.class);
        if (event == null || event.taskId() == null || event.output() == null || event.attempt() < 1) {
            throw new IllegalArgumentException("Invalid evaluation event");
        }
        UUID.fromString(event.taskId());
        evaluations.fromResult(event);
    }
}
