package io.github.deepwhalelabs.eventdrivenllm;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "app.jobs-enabled", havingValue = "true", matchIfMissing = true)
public class EvaluationJobs {
    private final EvaluationStore store;
    private final JevClient client;
    public EvaluationJobs(EvaluationStore store, JevClient client) { this.store = store; this.client = client; }

    @Scheduled(fixedDelay = 1000)
    public void evaluateNext() {
        if (!client.available()) return;
        store.claim(System.currentTimeMillis()).ifPresent(job -> {
            try { store.complete(job, client.evaluate(job)); }
            catch (JevClient.EvaluationException ex) { store.fail(job, ex, System.currentTimeMillis()); }
        });
    }
}
