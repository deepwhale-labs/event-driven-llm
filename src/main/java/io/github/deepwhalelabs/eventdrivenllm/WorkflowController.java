package io.github.deepwhalelabs.eventdrivenllm;

import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class WorkflowController {
    private final WorkflowService workflows;
    public WorkflowController(WorkflowService workflows) { this.workflows = workflows; }

    @PostMapping("/api/workflows")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public WorkflowService.Workflow create(@RequestBody Request request) {
        try { return workflows.create(request.prompt(), request.targetNode(), request.initialDraft()); }
        catch (IllegalArgumentException ex) { throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage()); }
    }

    @GetMapping("/api/workflows")
    public List<WorkflowService.Workflow> list(@RequestParam(defaultValue = "10") int limit) {
        if (limit < 1 || limit > 50) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit must be 1 to 50");
        return workflows.list(limit);
    }

    @GetMapping("/api/workflows/{id}")
    public WorkflowService.Workflow get(@PathVariable UUID id) { return workflows.get(id.toString()); }

    @PostMapping("/api/workflows/{id}/retry")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public WorkflowService.Workflow retry(@PathVariable UUID id) { return workflows.retry(id.toString()); }

    public record Request(String prompt, String targetNode, String initialDraft) { }
}
