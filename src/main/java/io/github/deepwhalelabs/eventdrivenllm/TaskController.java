package io.github.deepwhalelabs.eventdrivenllm;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class TaskController {
    private final TaskStore tasks;
    private final Routing routing;
    public TaskController(TaskStore tasks, Routing routing) { this.tasks = tasks; this.routing = routing; }

    @GetMapping("/api/tasks")
    public List<Task> list(@RequestParam(required = false) String status, @RequestParam(defaultValue = "30") int limit) {
        if (limit < 1 || limit > 100 || (status != null && !Set.of("QUEUED", "RUNNING", "DELIVERING", "SUCCEEDED", "FAILED").contains(status))) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid status or limit (1..100)");
        }
        return tasks.list(status, limit);
    }

    @GetMapping("/api/tasks/{id}")
    public Task get(@PathVariable UUID id) { return tasks.get(id.toString()); }

    @PostMapping("/api/tasks/{id}/retry")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Task retry(@PathVariable UUID id) { return tasks.retry(id.toString()); }

    @GetMapping("/api/nodes")
    public List<String> nodes() { return routing.nodes(); }

    @GetMapping("/api/batches")
    public List<TaskStore.BatchSummary> batches(@RequestParam(defaultValue = "10") int limit) {
        if (limit < 1 || limit > 50) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid limit (1..50)");
        return tasks.batches(limit);
    }

    @GetMapping("/api/batches/{id}")
    public TaskStore.Batch batch(@PathVariable UUID id) { return tasks.batch(id.toString()); }
}
