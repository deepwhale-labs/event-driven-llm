package io.github.deepwhalelabs.eventdrivenllm;

import java.util.Arrays;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Routing {
    private final String commandTopic;
    private final String resultTopic;
    private final String nodeId;
    private final List<String> nodes;

    public Routing(@Value("${app.kafka.command-topic}") String commandTopic,
            @Value("${app.kafka.result-topic}") String resultTopic,
            @Value("${app.node-id}") String nodeId, @Value("${app.worker-nodes}") String nodes) {
        this.commandTopic = commandTopic;
        this.resultTopic = resultTopic;
        this.nodeId = nodeId;
        this.nodes = Arrays.stream(nodes.split(",")).map(String::trim).distinct().toList();
        if (this.nodes.isEmpty() || this.nodes.stream().anyMatch(n -> !n.matches("[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}"))
                || !this.nodes.contains(nodeId)) {
            throw new IllegalArgumentException("APP_NODE_ID must belong to the valid WORKER_NODES list");
        }
    }

    public String commandTopic(String targetNode) {
        if (targetNode == null) return commandTopic;
        if (!nodes.contains(targetNode)) throw new IllegalArgumentException("Unknown targetNode");
        return commandTopic + "." + targetNode;
    }

    public String[] workerTopics() { return new String[] {commandTopic, commandTopic(nodeId)}; }
    public String resultTopic() { return resultTopic; }
    public String nodeId() { return nodeId; }
    public List<String> nodes() { return nodes; }
}
