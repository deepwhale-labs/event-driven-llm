package io.github.deepwhalelabs.eventdrivenllm;

import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class ExperimentController {
    private final ExperimentService experiments;

    public ExperimentController(ExperimentService experiments) { this.experiments = experiments; }

    @PostMapping("/api/experiments")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ExperimentService.Experiment create(@RequestBody Request request) {
        try { return experiments.create(request.prompt(), request.initialDraft(), request.expectedOutput(), request.targetNode()); }
        catch (IllegalArgumentException ex) { throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage()); }
    }

    @GetMapping("/api/experiments")
    public List<ExperimentService.Experiment> list(@RequestParam(defaultValue = "10") int limit) {
        if (limit < 1 || limit > 50) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit must be 1 to 50");
        return experiments.list(limit);
    }

    @GetMapping("/api/experiments/{id}")
    public ExperimentService.Experiment get(@PathVariable UUID id) { return experiments.get(id.toString()); }

    public record Request(String prompt, String initialDraft, String expectedOutput, String targetNode) { }
}
