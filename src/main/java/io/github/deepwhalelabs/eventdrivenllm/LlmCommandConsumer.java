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
    private final WorkflowService workflows;

    public LlmCommandConsumer(InferenceService inference, ObjectMapper mapper, TaskStore tasks, Routing routing, WorkflowService workflows) {
        this.inference = inference;
        this.mapper = mapper;
        this.tasks = tasks;
        this.routing = routing;
        this.workflows = workflows;
    }

    @KafkaListener(topics = "#{@routing.workerTopics()}")
    public void consume(String payload) throws Exception {
        LlmCommand command = mapper.readValue(payload, LlmCommand.class);
        var claim = tasks.claim(command, routing.nodeId());
        if (claim.isEmpty()) return;
        try {
            workflows.beforeInference(command.taskId());
            long started = System.nanoTime();
            String output;
            try {
                String prompt = tasks.get(command.taskId()).prompt();
                String system = workflows.systemPrompt(command.taskId());
                output = system == null ? inference.generate(prompt) : inference.generate(system, prompt);
            }
            finally { workflows.afterInference(command.taskId(), (System.nanoTime() - started) / 1_000_000); }
            if (output == null || output.isBlank() || output.length() > 128000) {
                throw new IllegalStateException("Model output is empty or exceeds 128000 characters");
            }
            workflows.validateOutput(command.taskId(), output);
            tasks.complete(command, claim.get(), routing.nodeId(), output);
        } catch (RuntimeException ex) {
            tasks.release(command, claim.get());
            throw ex;
        }
    }
}
