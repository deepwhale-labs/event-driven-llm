package io.github.deepwhalelabs.eventdrivenllm;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

/** Durable independent evaluation jobs. HTTP calls never hold the task/database lock. */
@Service
public class EvaluationStore {
    static final int MAX_ATTEMPTS = 3;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final TaskStore tasks;
    private final ObjectMapper mapper;
    private final JevClient client;
    private final RowMapper<Evaluation> row;

    public EvaluationStore(JdbcTemplate jdbc, PlatformTransactionManager transactions, TaskStore tasks,
            ObjectMapper mapper, JevClient client) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(transactions);
        this.tasks = tasks;
        this.mapper = mapper;
        this.client = client;
        this.row = (rs, n) -> new Evaluation(rs.getString("evaluation_id"), rs.getString("task_id"), rs.getInt("task_attempt"),
                rs.getString("generation"), rs.getString("trigger_kind"), rs.getString("status"), rs.getInt("attempts"),
                rs.getString("requested_model"), rs.getString("rubric_version"), rs.getDouble("threshold"),
                rs.getString("source_prompt"), rs.getString("source_output"), rs.getString("claim_token"),
                rs.getString("last_error"), rs.getLong("created_at"), rs.getObject("started_at", Long.class),
                rs.getObject("completed_at", Long.class), rs.getLong("next_attempt_at"), decode(rs.getString("report")));
    }

    public List<Evaluation> list(String taskId) {
        tasks.get(taskId);
        return jdbc.query("SELECT * FROM task_evaluations WHERE task_id=? ORDER BY created_at DESC,evaluation_id LIMIT 20", row, taskId);
    }

    public Evaluation request(String taskId) {
        if (!client.available()) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Jev evaluation is not configured");
        return enqueue(taskId, null);
    }

    public void fromResult(LlmResult event) { enqueue(event.taskId(), event); }

    private Evaluation enqueue(String taskId, LlmResult event) {
        return tx.execute(s -> {
            if (jdbc.query("SELECT task_id FROM tasks WHERE task_id=? FOR UPDATE", (rs, n) -> rs.getString(1), taskId).isEmpty()) {
                if (event != null) return null;
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found");
            }
            Task task = tasks.get(taskId);
            boolean saved = task.output() != null && (task.status().equals("SUCCEEDED") || task.status().equals("DELIVERING")
                    || (task.status().equals("FAILED") && "DELIVERY".equals(task.failureStage())));
            if (!saved || (event != null && (task.attempt() != event.attempt() || !task.output().equals(event.output())))) {
                if (event != null) return null; // A stale generation must not acquire the current task's result.
                throw new ResponseStatusException(HttpStatus.CONFLICT, "A saved inference result is required");
            }
            String generation = generation(task);
            var existing = jdbc.query("SELECT * FROM task_evaluations WHERE task_id=? AND generation=?"
                    + (event == null ? " AND status IN ('QUEUED','RUNNING')" : "")
                    + " ORDER BY created_at DESC,evaluation_id LIMIT 1", row, taskId, generation);
            if (!existing.isEmpty()) return existing.getFirst();
            var config = client.configuration();
            long now = System.currentTimeMillis();
            String id = UUID.randomUUID().toString();
            jdbc.update("INSERT INTO task_evaluations(evaluation_id,task_id,task_attempt,generation,trigger_kind,status,attempts,"
                    + "requested_model,rubric_version,threshold,source_prompt,source_output,created_at,next_attempt_at) "
                    + "VALUES (?,?,?,?,?,'QUEUED',0,?,?,?,?,?,?,?)", id, taskId, task.attempt(), generation,
                    event == null ? "MANUAL" : "AUTO", config.model(), config.rubricVersion(), config.threshold(), task.prompt(), task.output(), now, now);
            return get(id);
        });
    }

    private static String generation(Task task) {
        try {
            String source = task.inferenceCompletedAt() + "\n" + task.prompt() + "\n" + task.output();
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }

    public Evaluation get(String id) { return jdbc.queryForObject("SELECT * FROM task_evaluations WHERE evaluation_id=?", row, id); }

    public Optional<Evaluation> claim(long now) {
        return tx.execute(s -> {
            jdbc.update("UPDATE task_evaluations SET status=CASE WHEN attempts<? THEN 'QUEUED' ELSE 'FAILED' END,"
                    + "completed_at=CASE WHEN attempts<? THEN NULL ELSE ? END,claim_token=NULL,lease_until=NULL,"
                    + "last_error='WORKER_LEASE_EXPIRED',next_attempt_at=? WHERE status='RUNNING' AND lease_until<?",
                    MAX_ATTEMPTS, MAX_ATTEMPTS, now, now, now);
            var pending = jdbc.query("SELECT * FROM task_evaluations WHERE status='QUEUED' AND next_attempt_at<=? "
                    + "ORDER BY created_at,evaluation_id LIMIT 1 FOR UPDATE SKIP LOCKED", row, now);
            if (pending.isEmpty()) return Optional.empty();
            Evaluation job = pending.getFirst();
            jdbc.update("UPDATE task_evaluations SET status='RUNNING',attempts=attempts+1,claim_token=?,lease_until=?,started_at=? WHERE evaluation_id=?",
                    UUID.randomUUID().toString(), now + 60000, now, job.id());
            return Optional.of(get(job.id()));
        });
    }

    public void complete(Evaluation job, JevClient.Report report) {
        try {
            jdbc.update("UPDATE task_evaluations SET status='COMPLETED',report=?,completed_at=?,last_error=NULL,claim_token=NULL,lease_until=NULL "
                    + "WHERE evaluation_id=? AND status='RUNNING' AND claim_token=?",
                    mapper.writeValueAsString(report), System.currentTimeMillis(), job.id(), job.claimToken());
        } catch (JsonProcessingException ex) { throw new IllegalStateException("Cannot serialize evaluation", ex); }
    }

    public void fail(Evaluation job, JevClient.EvaluationException error, long now) {
        boolean retry = error.retryable && job.attempts() < MAX_ATTEMPTS;
        long delay = Math.max(5000L << Math.min(job.attempts() - 1, 4), error.retryAfterMillis);
        jdbc.update("UPDATE task_evaluations SET status=?,last_error=?,next_attempt_at=?,completed_at=?,claim_token=NULL,lease_until=NULL "
                + "WHERE evaluation_id=? AND status='RUNNING' AND claim_token=?", retry ? "QUEUED" : "FAILED", error.getMessage(),
                now + delay, retry ? null : now, job.id(), job.claimToken());
    }

    private JevClient.Report decode(String json) {
        if (json == null) return null;
        try { return mapper.readValue(json, JevClient.Report.class); }
        catch (JsonProcessingException ex) { throw new IllegalStateException("Invalid saved evaluation", ex); }
    }
}
