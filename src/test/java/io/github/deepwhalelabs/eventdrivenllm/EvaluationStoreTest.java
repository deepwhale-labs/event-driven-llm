package io.github.deepwhalelabs.eventdrivenllm;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.web.server.ResponseStatusException;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class EvaluationStoreTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private JdbcTemplate jdbc;
    private DataSourceTransactionManager tx;
    private TaskStore tasks;
    private EvaluationStore evaluations;
    private JevClient client;

    @BeforeEach
    void setup() {
        var source = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000", "sa", "");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(source);
        jdbc = new JdbcTemplate(source);
        tx = new DataSourceTransactionManager(source);
        tasks = new TaskStore(jdbc, tx, mapper, new Routing("commands", "results", "one", "one"), 1200000);
        client = mock(JevClient.class);
        when(client.available()).thenReturn(true);
        when(client.configuration()).thenReturn(new JevClient.Configuration("SHADOW", true, true, "jev-1.13.0", JevClient.RUBRIC_VERSION, .85));
        evaluations = newStore();
    }

    private EvaluationStore newStore() { return new EvaluationStore(jdbc, tx, tasks, mapper, client); }
    private Task generated() {
        Task task = tasks.create("회의 시간은 오후 3시입니다. 한 문장으로 안내하세요.", null);
        var event = new LlmCommand(task.taskId(), task.prompt(), 1);
        tasks.complete(event, tasks.claim(event, "one").orElseThrow(), "one", "회의 시간은 오전 3시입니다.");
        return tasks.get(task.taskId());
    }
    private LlmResult event(Task task) { return new LlmResult(task.taskId(), task.nodeId(), task.output(), task.attempt()); }
    private JevClient.Report report() { return new JevClient.Report("jev-1.13.0", JevClient.RUBRIC_VERSION, .85, "REJECT", List.of()); }

    @Test
    void automaticEventsAreDeduplicatedAcrossDeliveryRetriesAndDoNotChangeDelivery() {
        Task task = generated();
        evaluations.fromResult(event(task));
        evaluations.fromResult(event(task));
        assertThat(evaluations.list(task.taskId())).hasSize(1);
        var claim = evaluations.claim(System.currentTimeMillis()).orElseThrow();
        evaluations.complete(claim, report());
        tasks.fail(task.taskId(), task.attempt(), "DELIVERY");
        Task retried = tasks.retry(task.taskId());
        evaluations.fromResult(event(retried));
        assertThat(evaluations.list(task.taskId())).hasSize(1);
        assertThat(tasks.get(task.taskId()).status()).isEqualTo("DELIVERING");
        assertThat(tasks.get(task.taskId()).output()).isEqualTo(task.output());
        var saved = newStore().list(task.taskId()).getFirst();
        assertThat(saved.report().verdict()).isEqualTo("REJECT");
        assertThat(saved.output()).isEqualTo(task.output());
    }

    @Test
    void concurrentManualRequestsShareOneActiveJobButCompletedJobsCanBeReevaluated() throws Exception {
        Task task = generated();
        var start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var one = workers.submit(() -> { start.await(); return evaluations.request(task.taskId()); });
            var two = workers.submit(() -> { start.await(); return newStore().request(task.taskId()); });
            start.countDown();
            assertThat(one.get().id()).isEqualTo(two.get().id());
        }
        var claim = evaluations.claim(System.currentTimeMillis()).orElseThrow();
        assertThat(newStore().claim(System.currentTimeMillis())).isEmpty();
        evaluations.complete(claim, report());
        Evaluation fresh = evaluations.request(task.taskId());
        assertThat(fresh.id()).isNotEqualTo(claim.id());
        assertThat(evaluations.list(task.taskId())).hasSize(2);
        assertThat(mapper.writeValueAsString(fresh)).doesNotContain("회의 시간", "\"claimToken\":", "\"output\":", "\"prompt\":");
    }

    @Test
    void staleEventsCannotEvaluateANewerOrUnfinishedGeneration() {
        Task task = generated();
        evaluations.fromResult(new LlmResult(task.taskId(), "one", "stale", 1));
        evaluations.fromResult(new LlmResult(task.taskId(), "one", task.output(), 9));
        assertThat(evaluations.list(task.taskId())).isEmpty();
        Task pending = tasks.create("pending", null);
        assertThatThrownBy(() -> evaluations.request(pending.taskId())).isInstanceOf(ResponseStatusException.class);
        evaluations.fromResult(new LlmResult(UUID.randomUUID().toString(), "one", "absent", 1));
        when(client.available()).thenReturn(false);
        assertThatThrownBy(() -> evaluations.request(task.taskId())).isInstanceOf(ResponseStatusException.class);
        assertThat(evaluations.list(task.taskId())).isEmpty();
    }

    @Test
    void crashedWorkerCanBeRecoveredAndItsLateReportIsFenced() {
        Task task = generated();
        evaluations.request(task.taskId());
        long now = System.currentTimeMillis();
        var old = evaluations.claim(now).orElseThrow();
        var replacement = newStore().claim(now + 61000).orElseThrow();
        evaluations.complete(old, report());
        evaluations.fail(old, new JevClient.EvaluationException("CONNECTION_ERROR", true, 0), now);
        assertThat(evaluations.get(old.id()).status()).isEqualTo("RUNNING");
        assertThat(evaluations.get(old.id()).report()).isNull();
        evaluations.complete(replacement, report());
        assertThat(evaluations.get(old.id()).status()).isEqualTo("COMPLETED");
        assertThat(evaluations.get(old.id()).attempts()).isEqualTo(2);
    }

    @Test
    void retriesHonorBackoffAndLimitAndNeverRegenerateTheOriginalAnswer() {
        Task task = generated();
        Evaluation job = evaluations.request(task.taskId());
        long now = System.currentTimeMillis();
        var one = evaluations.claim(now).orElseThrow();
        evaluations.fail(one, new JevClient.EvaluationException("HTTP_429", true, 12000), now);
        assertThat(evaluations.claim(now + 11999)).isEmpty();
        var two = evaluations.claim(now + 12000).orElseThrow();
        evaluations.fail(two, new JevClient.EvaluationException("CONNECTION_ERROR", true, 0), now + 12000);
        assertThat(evaluations.claim(now + 21999)).isEmpty();
        var three = evaluations.claim(now + 22000).orElseThrow();
        evaluations.fail(three, new JevClient.EvaluationException("HTTP_529", true, 0), now + 22000);
        assertThat(evaluations.get(job.id()).status()).isEqualTo("FAILED");
        assertThat(evaluations.claim(now + 999999)).isEmpty();
        assertThat(tasks.get(task.taskId()).attempt()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE topic='commands'", Integer.class)).isEqualTo(1);
        var manual = evaluations.request(task.taskId());
        var last = evaluations.claim(System.currentTimeMillis()).orElseThrow();
        evaluations.fail(last, new JevClient.EvaluationException("HTTP_401", false, 0), now);
        assertThat(evaluations.get(manual.id()).status()).isEqualTo("FAILED");
    }
}
