package io.github.deepwhalelabs.eventdrivenllm;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class CommandController {
    private final TaskStore tasks;
    public CommandController(TaskStore tasks) { this.tasks = tasks; }

    @PostMapping("/api/commands")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> publish(@RequestBody CommandRequest request) {
        if (request.prompt() == null || request.prompt().isBlank() || request.prompt().length() > 32000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "prompt must contain 1 to 32000 characters");
        }
        try {
            Task task = tasks.create(request.prompt(), request.targetNode());
            return Map.of("taskId", task.taskId(), "status", task.status());
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
        }
    }

    public record CommandRequest(String prompt, String targetNode) { }
}
