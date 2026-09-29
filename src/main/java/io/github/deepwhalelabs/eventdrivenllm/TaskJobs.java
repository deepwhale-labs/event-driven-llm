package io.github.deepwhalelabs.eventdrivenllm;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "app.jobs-enabled", havingValue = "true", matchIfMissing = true)
public class TaskJobs {
    private static final Logger log = LoggerFactory.getLogger(TaskJobs.class);
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final TaskStore tasks;
    private final int maxRetries;
    private final long retryDelay;

    public TaskJobs(JdbcTemplate jdbc, PlatformTransactionManager transactions, KafkaTemplate<String, String> kafka,
            TaskStore tasks, @Value("${app.auto-retry.max-retries:2}") int maxRetries,
            @Value("${app.auto-retry.delay-ms:60000}") long retryDelay) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(transactions);
        this.kafka = kafka;
        this.tasks = tasks;
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
        if (maxRetries < 0 || retryDelay < 1000) throw new IllegalArgumentException("Invalid retry settings");
    }

    @Scheduled(fixedDelayString = "${app.outbox-delay-ms:1000}")
    public void publish() {
        try {
            publishBatch();
        } catch (RuntimeException ex) {
            log.warn("Outbox publication deferred; events remain in the database ({})", ex.getClass().getSimpleName());
        }
    }

    void publishBatch() {
        tx.executeWithoutResult(s -> {
            var events = jdbc.query("SELECT * FROM outbox ORDER BY created_at,event_id LIMIT 20 FOR UPDATE SKIP LOCKED",
                    (rs, n) -> new Event(rs.getString("event_id"), rs.getString("task_id"), rs.getString("topic"), rs.getString("payload")));
            for (var event : events) {
                try {
                    kafka.send(event.topic(), event.taskId(), event.payload()).get(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Kafka publication interrupted");
                } catch (Exception ex) {
                    throw new IllegalStateException("Kafka publication failed");
                }
                jdbc.update("DELETE FROM outbox WHERE event_id=?", event.id());
            }
        });
    }

    @Scheduled(fixedDelayString = "${app.recovery-delay-ms:5000}")
    public void recover() { tasks.recover(System.currentTimeMillis(), maxRetries, retryDelay); }

    private record Event(String id, String taskId, String topic, String payload) { }
}
