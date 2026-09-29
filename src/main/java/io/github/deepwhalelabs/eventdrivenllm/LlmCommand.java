package io.github.deepwhalelabs.eventdrivenllm;

public record LlmCommand(String taskId, String prompt, int attempt) {
    public LlmCommand { if (attempt == 0) attempt = 1; }
    public LlmCommand(String taskId, String prompt) { this(taskId, prompt, 1); }
}
