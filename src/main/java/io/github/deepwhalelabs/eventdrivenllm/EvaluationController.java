package io.github.deepwhalelabs.eventdrivenllm;

import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EvaluationController {
    private final EvaluationStore evaluations;
    private final JevClient client;
    public EvaluationController(EvaluationStore evaluations, JevClient client) { this.evaluations = evaluations; this.client = client; }

    @GetMapping("/api/evaluations/config")
    public JevClient.Configuration configuration() { return client.configuration(); }

    @GetMapping("/api/tasks/{id}/evaluations")
    public List<Evaluation> list(@PathVariable UUID id) { return evaluations.list(id.toString()); }

    @PostMapping("/api/tasks/{id}/evaluations")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Evaluation request(@PathVariable UUID id) { return evaluations.request(id.toString()); }
}
