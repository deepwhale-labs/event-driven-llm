package io.github.deepwhalelabs.eventdrivenllm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "app.worker-enabled", havingValue = "true", matchIfMissing = true)
public class LlmCommandConsumer {
    private final InferenceService inference;
    private final ObjectMapper mapper;
    private final TaskStore tasks;
    private final Routing routing;

    public LlmCommandConsumer(InferenceService inference, ObjectMapper mapper, TaskStore tasks, Routing routing) {
        this.inference = inference;
        this.mapper = mapper;
        this.tasks = tasks;
        this.routing = routing;
    }

    @KafkaListener(topics = "#{@routing.workerTopics()}")
    public void consume(String payload) throws Exception {
        LlmCommand command = mapper.readValue(payload, LlmCommand.class);
        var claim = tasks.claim(command, routing.nodeId());
        if (claim.isEmpty()) return;
        try {
            String output = inference.generate(tasks.get(command.taskId()).prompt());
            if (output == null || output.isBlank() || output.length() > 128000) {
                throw new IllegalStateException("Model output is empty or exceeds 128000 characters");
            }
            tasks.complete(command, claim.get(), routing.nodeId(), output);
        } catch (RuntimeException ex) {
            tasks.release(command, claim.get());
            throw ex;
        }
    }
}
