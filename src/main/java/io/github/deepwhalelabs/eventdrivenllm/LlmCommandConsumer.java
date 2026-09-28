package io.github.deepwhalelabs.eventdrivenllm;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class LlmCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(LlmCommandConsumer.class);

    private final InferenceService inference;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String resultTopic;
    private final String nodeId;

    public LlmCommandConsumer(InferenceService inference, KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper, @Value("${app.kafka.result-topic}") String resultTopic,
            @Value("${app.node-id}") String nodeId) {
        this.inference = inference;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.resultTopic = resultTopic;
        this.nodeId = nodeId;
    }

    @KafkaListener(topics = "${app.kafka.command-topic}")
    public void consume(String payload) throws Exception {
        LlmCommand command = objectMapper.readValue(payload, LlmCommand.class);
        String output = inference.generate(command.prompt());
        String result = objectMapper.writeValueAsString(new LlmResult(command.taskId(), nodeId, output));
        try {
            kafkaTemplate.send(resultTopic, command.taskId(), result).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ex;
        }
        log.info("Completed task {} on node {}", command.taskId(), nodeId);
    }
}
