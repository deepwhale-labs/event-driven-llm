package io.github.deepwhalelabs.eventdrivenllm;

public record LlmResult(String taskId, String nodeId, String output, int attempt) {
    public LlmResult { if (attempt == 0) attempt = 1; }
    public LlmResult(String taskId, String nodeId, String output) { this(taskId, nodeId, output, 1); }
}
