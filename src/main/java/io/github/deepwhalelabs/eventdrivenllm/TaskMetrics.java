package io.github.deepwhalelabs.eventdrivenllm;

import java.util.List;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class TaskMetrics {
    public TaskMetrics(MeterRegistry meters, JdbcTemplate jdbc) {
        for (String status : List.of("QUEUED", "RUNNING", "DELIVERING", "SUCCEEDED", "FAILED")) {
            Gauge.builder("llm.tasks", jdbc, db -> db.queryForObject("SELECT COUNT(*) FROM tasks WHERE status=?", Long.class, status))
                    .tag("status", status).register(meters);
        }
        Gauge.builder("llm.outbox.pending", jdbc, db -> db.queryForObject("SELECT COUNT(*) FROM outbox", Long.class)).register(meters);
    }
}
