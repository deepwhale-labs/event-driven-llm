package io.github.deepwhalelabs.eventdrivenllm;

import java.util.Map;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class ResultController {
    private final CoralClient coral;
    private final TaskStore tasks;

    public ResultController(CoralClient coral, TaskStore tasks) {
        this.coral = coral;
        this.tasks = tasks;
    }

    @GetMapping("/api/coral")
    public Map<String, String> status() {
        return tasks.withCoralLock(coral::status);
    }

    @GetMapping("/api/results/{taskId}")
    public CoralClient.Delivery result(@PathVariable UUID taskId) {
        Task task = tasks.get(taskId.toString());
        if (task.output() == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Result not available yet; query /api/tasks/{taskId}");
        return new CoralClient.Delivery(task.taskId(), task.threadId(), task.nodeId(), task.output());
    }

    @ExceptionHandler(CoralException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> unavailable() {
        return Map.of("error", "Coral unavailable");
    }
}
