package io.github.deepwhalelabs.eventdrivenllm;

public record LlmResult(String taskId, String nodeId, String output) {
}
