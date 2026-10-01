package io.github.deepwhalelabs.eventdrivenllm;

import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ExperimentService {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final WorkflowService workflows;

    public ExperimentService(JdbcTemplate jdbc, PlatformTransactionManager transactions, WorkflowService workflows) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(transactions);
        this.workflows = workflows;
    }

    public Experiment create(String prompt, String initialDraft, String expectedOutput, String targetNode) {
        WorkflowService.validate(prompt);
        WorkflowService.validate(initialDraft);
        if (expectedOutput != null) WorkflowService.validate(expectedOutput);
        return tx.execute(s -> {
            String id = UUID.randomUUID().toString();
            long createdAt = System.currentTimeMillis();
            // Both arms start from the same supplied draft, with independent Coral threads.
            var direct = workflows.create(prompt, targetNode, initialDraft, "DIRECT");
            var review = workflows.create(prompt, targetNode, initialDraft, "REVIEW");
            jdbc.update("INSERT INTO workflow_experiments(experiment_id,expected_output,direct_workflow_id,review_workflow_id,created_at) VALUES (?,?,?,?,?)",
                    id, expectedOutput, direct.workflowId(), review.workflowId(), createdAt);
            return get(id);
        });
    }

    public Experiment get(String id) {
        var definitions = jdbc.query("SELECT * FROM workflow_experiments WHERE experiment_id=?", (rs, n) -> new Definition(
                rs.getString("experiment_id"), rs.getString("expected_output"), rs.getString("direct_workflow_id"),
                rs.getString("review_workflow_id"), rs.getLong("created_at")), id);
        if (definitions.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Experiment not found");
        var definition = definitions.getFirst();
        var direct = workflows.get(definition.directId());
        var review = workflows.get(definition.reviewId());
        String draft = direct.steps().getFirst().task().output();
        String status = direct.status().equals("SUCCEEDED") && review.status().equals("SUCCEEDED") ? "SUCCEEDED"
                : direct.status().equals("FAILED") || review.status().equals("FAILED") ? "FAILED" : "RUNNING";
        return new Experiment(id, direct.prompt(), draft, definition.expected(), direct.targetNode(), definition.createdAt(),
                status, matches(draft, definition.expected()), arm(direct, definition.expected()), arm(review, definition.expected()));
    }

    public List<Experiment> list(int limit) {
        return jdbc.query("SELECT experiment_id FROM workflow_experiments ORDER BY created_at DESC,experiment_id LIMIT ?",
                (rs, n) -> rs.getString(1), limit).stream().map(this::get).toList();
    }

    private Arm arm(WorkflowService.Workflow workflow, String expected) {
        var last = workflow.steps().getLast();
        boolean done = workflow.status().equals("SUCCEEDED");
        String output = last.stage().equals("REVISION") ? last.task().output() : null;
        Long elapsed = done && last.task().finishedAt() != null
                ? Math.max(0, last.task().finishedAt() - workflow.createdAt()) : null;
        return new Arm(workflow, output, done ? matches(output, expected) : null, elapsed);
    }

    // Exact-match scoring only: remove surrounding whitespace, preserve punctuation and internal spacing.
    private static Boolean matches(String actual, String expected) {
        return actual == null || expected == null ? null : actual.strip().equals(expected.strip());
    }

    private record Definition(String id, String expected, String directId, String reviewId, long createdAt) { }
    public record Arm(WorkflowService.Workflow workflow, String output, Boolean matchesExpected, Long elapsedMillis) { }
    public record Experiment(String experimentId, String prompt, String initialDraft, String expectedOutput, String targetNode,
            long createdAt, String status, Boolean draftMatchesExpected, Arm direct, Arm review) { }
}
