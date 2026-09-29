package io.github.deepwhalelabs.eventdrivenllm;

import com.fasterxml.jackson.annotation.JsonIgnore;

public record Task(String taskId, String prompt, String targetNode, String status, int attempt,
        String nodeId, String output, String threadId, String failureStage, String lastError,
        @JsonIgnore String claimToken, @JsonIgnore Long leaseUntil, long createdAt, long updatedAt,
        String batchId, Long queuedAt, Long startedAt, Long inferenceCompletedAt, Long finishedAt) {
}
