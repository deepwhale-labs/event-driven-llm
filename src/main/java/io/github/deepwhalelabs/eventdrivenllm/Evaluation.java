package io.github.deepwhalelabs.eventdrivenllm;

import com.fasterxml.jackson.annotation.JsonIgnore;

public record Evaluation(String id, String taskId, int taskAttempt, String generation, String trigger,
        String status, int attempts, String requestedModel, String rubricVersion, double threshold,
        @JsonIgnore String prompt, @JsonIgnore String output, @JsonIgnore String claimToken,
        String lastError, long createdAt, Long startedAt, Long completedAt, long nextAttemptAt,
        JevClient.Report report) { }
