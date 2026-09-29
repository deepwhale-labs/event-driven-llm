package io.github.deepwhalelabs.eventdrivenllm;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

@Service
public class TaskStore {
    private static final RowMapper<Task> ROW = (rs, n) -> new Task(rs.getString("task_id"),
            rs.getString("prompt"), rs.getString("target_node"), rs.getString("status"), rs.getInt("attempt"),
            rs.getString("node_id"), rs.getString("output"), rs.getString("thread_id"),
            rs.getString("failure_stage"), rs.getString("last_error"), rs.getString("claim_token"),
            rs.getObject("lease_until", Long.class), rs.getLong("created_at"), rs.getLong("updated_at"),
            rs.getString("batch_id"), rs.getObject("queued_at", Long.class), rs.getObject("started_at", Long.class),
            rs.getObject("inference_completed_at", Long.class), rs.getObject("finished_at", Long.class));
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper mapper;
    private final Routing routing;
    private final long leaseMillis;

    public TaskStore(JdbcTemplate jdbc, PlatformTransactionManager transactions, ObjectMapper mapper,
            Routing routing, @Value("${app.processing-lease-ms:1200000}") long leaseMillis) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(transactions);
        this.mapper = mapper;
        this.routing = routing;
        this.leaseMillis = leaseMillis;
        if (leaseMillis < 1000) throw new IllegalArgumentException("Processing lease must be at least 1000 ms");
    }

    public Task create(String prompt, String targetNode) {
        String topic = routing.commandTopic(targetNode);
        return tx.execute(s -> insert(prompt, targetNode, topic, null));
    }

    public Batch createBatch(List<String> prompts, String targetNode) {
        String topic = routing.commandTopic(targetNode);
        return tx.execute(s -> {
            String batchId = UUID.randomUUID().toString();
            for (String prompt : prompts) insert(prompt, targetNode, topic, batchId);
            return batch(batchId);
        });
    }

    private Task insert(String prompt, String targetNode, String topic, String batchId) {
        String id = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        jdbc.update("INSERT INTO tasks(task_id,prompt,target_node,status,created_at,updated_at,batch_id,queued_at) VALUES (?,?,?,'QUEUED',?,?,?,?)",
                id, prompt, targetNode, now, now, batchId, now);
        enqueue(id, topic, new LlmCommand(id, prompt, 1));
        return get(id);
    }

    public Batch batch(String id) {
        var items = jdbc.query("SELECT * FROM tasks WHERE batch_id=? ORDER BY created_at,task_id", ROW, id);
        if (items.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Batch not found");
        return new Batch(summarize(id, items), items);
    }

    public List<BatchSummary> batches(int limit) {
        return jdbc.query("SELECT batch_id,MIN(created_at) AS created_at,COUNT(*) AS total,"
                + "SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,"
                + "SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed "
                + "FROM tasks WHERE batch_id IS NOT NULL GROUP BY batch_id ORDER BY MIN(created_at) DESC,batch_id LIMIT ?",
                (rs, n) -> new BatchSummary(rs.getString("batch_id"), rs.getLong("created_at"), rs.getInt("total"),
                        rs.getInt("succeeded"), rs.getInt("failed")), limit);
    }

    private static BatchSummary summarize(String id, List<Task> items) {
        return new BatchSummary(id, items.stream().mapToLong(Task::createdAt).min().orElseThrow(), items.size(),
                (int) items.stream().filter(t -> t.status().equals("SUCCEEDED")).count(),
                (int) items.stream().filter(t -> t.status().equals("FAILED")).count());
    }

    public Task get(String id) {
        return find(id, false).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found"));
    }

    public List<Task> list(String status, int limit) {
        if (status == null) return jdbc.query("SELECT * FROM tasks ORDER BY created_at DESC,task_id LIMIT ?", ROW, limit);
        return jdbc.query("SELECT * FROM tasks WHERE status=? ORDER BY created_at DESC,task_id LIMIT ?", ROW, status, limit);
    }

    private Optional<Task> find(String id, boolean lock) {
        return jdbc.query("SELECT * FROM tasks WHERE task_id=?" + (lock ? " FOR UPDATE" : ""), ROW, id).stream().findFirst();
    }

    public Optional<String> claim(LlmCommand command, String nodeId) {
        return tx.execute(s -> {
            Task task = find(command.taskId(), true).orElse(null);
            if (task == null || task.attempt() != command.attempt() || !task.status().equals("QUEUED")) return Optional.empty();
            if (task.targetNode() != null && !task.targetNode().equals(nodeId)) {
                throw new IllegalArgumentException("Command delivered to the wrong worker");
            }
            String token = UUID.randomUUID().toString();
            long now = System.currentTimeMillis();
            jdbc.update("UPDATE tasks SET status='RUNNING',node_id=?,claim_token=?,lease_until=?,updated_at=?,started_at=?,inference_completed_at=NULL,finished_at=NULL WHERE task_id=?",
                    nodeId, token, now + leaseMillis, now, now, task.taskId());
            return Optional.of(token);
        });
    }

    public void complete(LlmCommand command, String token, String nodeId, String output) {
        tx.executeWithoutResult(s -> {
            long now = System.currentTimeMillis();
            int updated = jdbc.update("UPDATE tasks SET status='DELIVERING',output=?,claim_token=NULL,lease_until=NULL,last_error=NULL,updated_at=?,inference_completed_at=? "
                    + "WHERE task_id=? AND attempt=? AND claim_token=? AND status='RUNNING'",
                    output, now, now, command.taskId(), command.attempt(), token);
            if (updated == 1) enqueue(command.taskId(), routing.resultTopic(),
                    new LlmResult(command.taskId(), nodeId, output, command.attempt()));
        });
    }

    public void release(LlmCommand command, String token) {
        long now = System.currentTimeMillis();
        jdbc.update("UPDATE tasks SET status='QUEUED',claim_token=NULL,lease_until=NULL,updated_at=?,queued_at=?,started_at=NULL,inference_completed_at=NULL WHERE task_id=? AND attempt=? AND claim_token=?",
                now, now, command.taskId(), command.attempt(), token);
    }

    public void deliver(LlmResult event, CoralClient coral) {
        tx.executeWithoutResult(s -> {
            Task task = find(event.taskId(), true).orElse(null);
            if (task == null || task.attempt() != event.attempt() || !task.status().equals("DELIVERING")) return;
            // Serialize Coral session discovery and sends across application instances.
            coralLock();
            CoralClient.Delivery receipt = coral.deliver(new LlmResult(task.taskId(), task.nodeId(), task.output(), task.attempt()));
            long now = System.currentTimeMillis();
            jdbc.update("UPDATE tasks SET status='SUCCEEDED',thread_id=?,failure_stage=NULL,last_error=NULL,updated_at=?,finished_at=? WHERE task_id=?",
                    receipt.threadId(), now, now, task.taskId());
        });
    }

    public <T> T withCoralLock(Supplier<T> action) {
        return tx.execute(s -> { coralLock(); return action.get(); });
    }

    private void coralLock() {
        jdbc.queryForObject("SELECT id FROM coordination_lock WHERE id='coral' FOR UPDATE", String.class);
    }

    public void fail(String id, int attempt, String stage) {
        String expected = stage.equals("COMMAND") ? "QUEUED" : "DELIVERING";
        long now = System.currentTimeMillis();
        jdbc.update("UPDATE tasks SET status='FAILED',failure_stage=?,last_error=?,updated_at=?,finished_at=? WHERE task_id=? AND attempt=? AND status=?",
                stage, stage.equals("COMMAND") ? "Inference failed; see the command DLT" : "Coral delivery failed; see the result DLT",
                now, now, id, attempt, expected);
    }

    public Task retry(String id) {
        return tx.execute(s -> {
            Task task = find(id, true).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Task not found"));
            if (!task.status().equals("FAILED")) throw new ResponseStatusException(HttpStatus.CONFLICT, "Only failed tasks can be retried");
            requeue(task, "DELIVERY".equals(task.failureStage()));
            return get(id);
        });
    }

    public void recover(long now, int maxAutoRetries, long retryDelayMillis) {
        tx.executeWithoutResult(s -> {
            var tasks = jdbc.query("SELECT * FROM tasks WHERE (status='RUNNING' AND lease_until<?) "
                    + "OR (status='FAILED' AND attempt<=? AND updated_at<?) ORDER BY updated_at LIMIT 50 FOR UPDATE SKIP LOCKED",
                    ROW, now, maxAutoRetries, now - retryDelayMillis);
            for (Task task : tasks) {
                if (task.status().equals("RUNNING") && task.attempt() > maxAutoRetries) {
                    jdbc.update("UPDATE tasks SET status='FAILED',failure_stage='COMMAND',last_error='Worker lease expired',"
                            + "claim_token=NULL,lease_until=NULL,updated_at=?,finished_at=? WHERE task_id=?", now, now, task.taskId());
                } else {
                    requeue(task, "DELIVERY".equals(task.failureStage()));
                }
            }
        });
    }

    private void requeue(Task task, boolean deliveryOnly) {
        int attempt = task.attempt() + 1;
        long now = System.currentTimeMillis();
        jdbc.update("UPDATE tasks SET status=?,attempt=?,failure_stage=NULL,last_error=NULL,claim_token=NULL,lease_until=NULL,updated_at=?,finished_at=NULL,"
                + "queued_at=?,started_at=?,inference_completed_at=? WHERE task_id=?",
                deliveryOnly ? "DELIVERING" : "QUEUED", attempt, now, deliveryOnly ? task.queuedAt() : now,
                deliveryOnly ? task.startedAt() : null, deliveryOnly ? task.inferenceCompletedAt() : null, task.taskId());
        if (deliveryOnly) enqueue(task.taskId(), routing.resultTopic(), new LlmResult(task.taskId(), task.nodeId(), task.output(), attempt));
        else enqueue(task.taskId(), routing.commandTopic(task.targetNode()), new LlmCommand(task.taskId(), task.prompt(), attempt));
    }

    private void enqueue(String id, String topic, Object event) {
        try {
            jdbc.update("INSERT INTO outbox(event_id,task_id,topic,payload,created_at) VALUES (?,?,?,?,?)",
                    UUID.randomUUID().toString(), id, topic, mapper.writeValueAsString(event), System.currentTimeMillis());
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Cannot serialize task event", ex);
        }
    }

    public record BatchSummary(String batchId, long createdAt, int total, int succeeded, int failed) { }
    public record Batch(BatchSummary summary, List<Task> tasks) { }
}
