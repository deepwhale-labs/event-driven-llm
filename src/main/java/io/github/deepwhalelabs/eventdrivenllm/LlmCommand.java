package io.github.deepwhalelabs.eventdrivenllm;

public record LlmCommand(String taskId, String prompt) {
}
