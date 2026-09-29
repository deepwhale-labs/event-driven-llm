package io.github.deepwhalelabs.eventdrivenllm;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.server.ResponseStatusException;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TaskStoreTest {
    private JdbcTemplate jdbc;
    private DataSourceTransactionManager transactions;
    private TaskStore store;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Routing routing = new Routing("commands", "results", "one", "one,two");

    @BeforeEach
    void setup() {
        var source = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000", "sa", "");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(source);
        jdbc = new JdbcTemplate(source);
        transactions = new DataSourceTransactionManager(source);
        store = newStore();
    }

    private TaskStore newStore() { return new TaskStore(jdbc, transactions, mapper, routing, 1200000); }
    private LlmCommand command(Task task) { return new LlmCommand(task.taskId(), task.prompt(), task.attempt()); }
    private LlmResult result(Task task) { return new LlmResult(task.taskId(), task.nodeId(), task.output(), task.attempt()); }
    private Task infer(Task task) {
        String claim = store.claim(command(task), "one").orElseThrow();
        store.complete(command(task), claim, "one", "Saved output");
        return store.get(task.taskId());
    }

    @Test
    void storesRequestAndOutboxAtomicallyAndRejectsUnknownRoute() {
        Task task = store.create("hello", "two");
        assertThat(jdbc.queryForObject("SELECT topic FROM outbox", String.class)).isEqualTo("commands.two");
        assertThat(newStore().get(task.taskId()).prompt()).isEqualTo("hello");
        assertThatThrownBy(() -> store.create("hello", "missing")).isInstanceOf(IllegalArgumentException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM tasks", Integer.class)).isEqualTo(1);
        assertThatThrownBy(() -> store.claim(command(task), "one")).isInstanceOf(IllegalArgumentException.class);
        assertThat(store.get(task.taskId()).status()).isEqualTo("QUEUED");
    }

    @Test
    void serializationFailureRollsBackAcceptedTask() throws Exception {
        var brokenMapper = mock(ObjectMapper.class);
        when(brokenMapper.writeValueAsString(any())).thenThrow(new com.fasterxml.jackson.core.JsonProcessingException("bad") { });
        var brokenStore = new TaskStore(jdbc, transactions, brokenMapper, routing, 1200000);
        assertThatThrownBy(() -> brokenStore.create("hello", null)).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM tasks", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox", Integer.class)).isZero();
    }

    @Test
    void twoApplicationInstancesCannotClaimTheSameTask() throws Exception {
        Task task = store.create("hello", null);
        var secondStore = newStore();
        var start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> { start.await(); return store.claim(command(task), "one"); });
            var second = workers.submit(() -> { start.await(); return secondStore.claim(command(task), "two"); });
            start.countDown();
            assertThat((first.get().isPresent() ? 1 : 0) + (second.get().isPresent() ? 1 : 0)).isEqualTo(1);
        }
        assertThat(newStore().get(task.taskId()).status()).isEqualTo("RUNNING");
    }

    @Test
    void savesInferenceBeforeCoralAndDeduplicatesResultAfterRestart() {
        Task task = infer(store.create("hello", null));
        assertThat(task.status()).isEqualTo("DELIVERING");
        var coral = mock(CoralClient.class);
        when(coral.deliver(any())).thenReturn(new CoralClient.Delivery(task.taskId(), "thread", "one", task.output()));
        store.deliver(result(task), coral);
        newStore().deliver(result(task), coral);
        verify(coral, times(1)).deliver(any());
        assertThat(newStore().get(task.taskId()).status()).isEqualTo("SUCCEEDED");
        assertThat(newStore().get(task.taskId()).output()).isEqualTo("Saved output");
        assertThat(newStore().get(task.taskId()).threadId()).isEqualTo("thread");
        assertThat(newStore().claim(command(task), "one")).isEmpty();
        assertThatThrownBy(() -> store.retry(task.taskId())).isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void coralCallsFromDifferentInstancesAreSerialized() throws Exception {
        Task first = infer(store.create("first", null));
        Task second = infer(store.create("second", null));
        var otherStore = newStore();
        var active = new AtomicInteger();
        var peak = new AtomicInteger();
        var coral = mock(CoralClient.class);
        when(coral.deliver(any())).thenAnswer(invocation -> {
            peak.accumulateAndGet(active.incrementAndGet(), Math::max);
            try {
                Thread.sleep(50);
                LlmResult event = invocation.getArgument(0);
                return new CoralClient.Delivery(event.taskId(), "thread", event.nodeId(), event.output());
            } finally { active.decrementAndGet(); }
        });
        try (var workers = Executors.newFixedThreadPool(2)) {
            var one = workers.submit(() -> store.deliver(result(first), coral));
            var two = workers.submit(() -> otherStore.deliver(result(second), coral));
            one.get();
            two.get();
        }
        assertThat(peak.get()).isEqualTo(1);
        assertThat(store.get(first.taskId()).status()).isEqualTo("SUCCEEDED");
        assertThat(store.get(second.taskId()).status()).isEqualTo("SUCCEEDED");
    }

    @Test
    void deliveryRetryReusesOutputAndIgnoresStaleResultsAndFailures() {
        Task task = infer(store.create("hello", null));
        var coral = mock(CoralClient.class);
        when(coral.deliver(any())).thenThrow(new CoralException("offline"));
        assertThatThrownBy(() -> store.deliver(result(task), coral)).isInstanceOf(CoralException.class);
        assertThat(store.get(task.taskId()).status()).isEqualTo("DELIVERING");
        store.fail(task.taskId(), 1, "DELIVERY");
        Task retry = store.retry(task.taskId());
        assertThat(retry.status()).isEqualTo("DELIVERING");
        assertThat(retry.output()).isEqualTo(task.output());
        assertThat(retry.attempt()).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE topic='commands'", Integer.class)).isEqualTo(1);
        store.fail(task.taskId(), 1, "DELIVERY");
        store.deliver(result(task), coral);
        verify(coral, times(1)).deliver(any());
        assertThat(store.get(task.taskId()).status()).isEqualTo("DELIVERING");
    }

    @Test
    void releasesFailedInferenceAndRetriesWithANewGeneration() throws Exception {
        Task task = store.create("hello", null);
        var inference = mock(InferenceService.class);
        when(inference.generate(anyString())).thenThrow(new IllegalStateException("model offline"));
        var consumer = new LlmCommandConsumer(inference, mapper, store, routing);
        assertThatThrownBy(() -> consumer.consume(mapper.writeValueAsString(command(task)))).isInstanceOf(IllegalStateException.class);
        assertThat(store.get(task.taskId()).status()).isEqualTo("QUEUED");
        store.fail(task.taskId(), 1, "COMMAND");
        Task retry = store.retry(task.taskId());
        assertThat(store.claim(command(task), "one")).isEmpty();
        assertThat(store.claim(command(retry), "one")).isPresent();
        store.fail(task.taskId(), 1, "COMMAND");
        assertThat(store.get(task.taskId()).status()).isEqualTo("RUNNING");
    }

    @Test
    void expiredWorkerIsRecoveredAndCannotOverwriteNewAttempt() {
        Task task = store.create("hello", null);
        String oldClaim = store.claim(command(task), "one").orElseThrow();
        jdbc.update("UPDATE tasks SET lease_until=0 WHERE task_id=?", task.taskId());
        store.recover(System.currentTimeMillis(), 2, 60000);
        Task retry = store.get(task.taskId());
        assertThat(retry.status()).isEqualTo("QUEUED");
        assertThat(retry.attempt()).isEqualTo(2);
        store.complete(command(task), oldClaim, "one", "stale output");
        assertThat(store.get(task.taskId()).output()).isNull();
        assertThat(store.claim(command(retry), "one")).isPresent();
        jdbc.update("UPDATE tasks SET attempt=3,lease_until=0 WHERE task_id=?", task.taskId());
        store.recover(System.currentTimeMillis(), 2, 60000);
        assertThat(store.get(task.taskId()).status()).isEqualTo("FAILED");
        assertThat(store.get(task.taskId()).lastError()).isEqualTo("Worker lease expired");
    }

    @Test
    void automaticRetryHonorsDelayAndLimit() {
        Task task = store.create("hello", null);
        store.fail(task.taskId(), 1, "COMMAND");
        store.recover(System.currentTimeMillis(), 2, 60000);
        assertThat(store.get(task.taskId()).status()).isEqualTo("FAILED");
        store.recover(System.currentTimeMillis() + 61000, 2, 60000);
        assertThat(store.get(task.taskId()).attempt()).isEqualTo(2);
        store.fail(task.taskId(), 2, "COMMAND");
        store.recover(System.currentTimeMillis() + 61000, 2, 60000);
        store.fail(task.taskId(), 3, "COMMAND");
        store.recover(System.currentTimeMillis() + 61000, 2, 60000);
        assertThat(store.get(task.taskId()).status()).isEqualTo("FAILED");
        assertThat(store.get(task.taskId()).attempt()).isEqualTo(3);
    }

    @Test
    @SuppressWarnings("unchecked")
    void outboxKeepsEventsWhenBrokerFailsAndRemovesOnlyAfterAcknowledgment() {
        store.create("hello", null);
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("offline")));
        var jobs = new TaskJobs(jdbc, transactions, kafka, store, 2, 60000);
        assertThatThrownBy(jobs::publishBatch).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox", Integer.class)).isEqualTo(1);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        jobs.publishBatch();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox", Integer.class)).isZero();
    }

    @Test
    void batchPersistsAllTasksAndOutboxWithAnIndependentSummary() {
        var batch = store.createBatch(java.util.List.of("first", "second", "third"), "one");
        store.create("separate", null);
        assertThat(batch.summary().total()).isEqualTo(3);
        assertThat(batch.tasks()).allMatch(t -> t.batchId().equals(batch.summary().batchId()) && t.queuedAt() != null);
        assertThat(batch.tasks()).extracting(Task::prompt).containsExactlyInAnyOrder("first", "second", "third");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE topic='commands.one'", Integer.class)).isEqualTo(3);
        var completed = infer(batch.tasks().getFirst());
        var coral = mock(CoralClient.class);
        when(coral.deliver(any())).thenReturn(new CoralClient.Delivery(completed.taskId(), "thread", "one", completed.output()));
        store.deliver(result(completed), coral);
        store.fail(batch.tasks().get(1).taskId(), 1, "COMMAND");
        var saved = newStore().batch(batch.summary().batchId());
        assertThat(saved.summary()).isEqualTo(new TaskStore.BatchSummary(batch.summary().batchId(), batch.summary().createdAt(), 3, 1, 1));
        assertThat(newStore().batches(10)).containsExactly(saved.summary());
        assertThatThrownBy(() -> store.batch(UUID.randomUUID().toString())).isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void aFailureHalfwayThroughABatchRollsBackEveryTaskAndOutboxEntry() throws Exception {
        var brokenMapper = spy(new ObjectMapper());
        when(brokenMapper.writeValueAsString(any())).thenCallRealMethod()
                .thenThrow(new com.fasterxml.jackson.core.JsonProcessingException("second event fails") { });
        var broken = new TaskStore(jdbc, transactions, brokenMapper, routing, 1200000);
        assertThatThrownBy(() -> broken.createBatch(java.util.List.of("first", "second", "third"), null)).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM tasks", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox", Integer.class)).isZero();
        assertThatThrownBy(() -> store.createBatch(java.util.List.of("first", "second"), "missing")).isInstanceOf(IllegalArgumentException.class);
        assertThat(store.batches(10)).isEmpty();
    }

    @Test
    void timingsSurviveDeliveryRetryAndAreResetForANewInference() {
        Task created = store.create("timing", null);
        assertThat(created.queuedAt()).isEqualTo(created.createdAt());
        assertThat(created.startedAt()).isNull();
        var generated = infer(created);
        assertThat(generated.startedAt()).isGreaterThanOrEqualTo(generated.queuedAt());
        assertThat(generated.inferenceCompletedAt()).isGreaterThanOrEqualTo(generated.startedAt());
        assertThat(generated.finishedAt()).isNull();
        store.fail(created.taskId(), 1, "DELIVERY");
        assertThat(store.get(created.taskId()).finishedAt()).isNotNull();
        var retried = store.retry(created.taskId());
        assertThat(retried.finishedAt()).isNull();
        assertThat(retried.startedAt()).isEqualTo(generated.startedAt());
        assertThat(retried.inferenceCompletedAt()).isEqualTo(generated.inferenceCompletedAt());
        var coral = mock(CoralClient.class);
        when(coral.deliver(any())).thenReturn(new CoralClient.Delivery(created.taskId(), "thread", "one", generated.output()));
        store.deliver(result(retried), coral);
        assertThat(store.get(created.taskId()).finishedAt()).isGreaterThanOrEqualTo(generated.inferenceCompletedAt());
        var failed = store.create("failed", null);
        String token = store.claim(command(failed), "one").orElseThrow();
        store.release(command(failed), token);
        store.fail(failed.taskId(), 1, "COMMAND");
        var again = store.retry(failed.taskId());
        assertThat(again.startedAt()).isNull();
        assertThat(again.inferenceCompletedAt()).isNull();
        assertThat(again.finishedAt()).isNull();
        store.complete(command(failed), token, "one", "late");
        assertThat(store.get(failed.taskId()).inferenceCompletedAt()).isNull();
    }

    @Test
    void additiveMigrationPreservesExistingRowsAndLeavesUnmeasuredTimesUnknown() {
        var source = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        var old = new JdbcTemplate(source);
        old.execute("CREATE TABLE tasks(task_id VARCHAR(36) PRIMARY KEY,prompt TEXT NOT NULL,target_node VARCHAR(64),status VARCHAR(24) NOT NULL,"
                + "attempt INTEGER NOT NULL DEFAULT 1,node_id VARCHAR(64),output TEXT,thread_id VARCHAR(36),failure_stage VARCHAR(16),last_error VARCHAR(160),"
                + "claim_token VARCHAR(36),lease_until BIGINT,created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL)");
        String id = UUID.randomUUID().toString();
        old.update("INSERT INTO tasks(task_id,prompt,status,output,created_at,updated_at) VALUES (?,?,'SUCCEEDED',?,1,2)", id, "legacy", "saved answer");
        var migration = new ResourceDatabasePopulator(new ClassPathResource("schema.sql"));
        migration.execute(source);
        migration.execute(source);
        var upgraded = new TaskStore(old, new DataSourceTransactionManager(source), mapper, routing, 1200000);
        Task task = upgraded.get(id);
        assertThat(task.output()).isEqualTo("saved answer");
        assertThat(task.batchId()).isNull();
        assertThat(task.startedAt()).isNull();
        assertThat(task.inferenceCompletedAt()).isNull();
        assertThat(task.finishedAt()).isNull();
    }
}
