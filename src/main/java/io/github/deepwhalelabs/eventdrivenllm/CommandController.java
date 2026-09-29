package io.github.deepwhalelabs.eventdrivenllm;

import java.util.Map;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class CommandController {
    static final int MAX_BATCH_SIZE = 50;
    static final int MAX_BATCH_CHARACTERS = 256000;
    private final TaskStore tasks;
    public CommandController(TaskStore tasks) { this.tasks = tasks; }

    @PostMapping("/api/commands")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> publish(@RequestBody CommandRequest request) {
        validatePrompt(request.prompt());
        try {
            Task task = tasks.create(request.prompt(), request.targetNode());
            return Map.of("taskId", task.taskId(), "status", task.status());
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
        }
    }

    @PostMapping("/api/commands/batch")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public TaskStore.Batch publishBatch(@RequestBody BatchRequest request) {
        if (request.prompts() == null || request.prompts().isEmpty() || request.prompts().size() > MAX_BATCH_SIZE) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A batch must contain 1 to 50 prompts");
        }
        int characters = 0;
        for (String prompt : request.prompts()) { validatePrompt(prompt); characters += prompt.length(); }
        if (characters > MAX_BATCH_CHARACTERS) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Batch exceeds 256000 characters");
        }
        try {
            return tasks.createBatch(request.prompts(), request.targetNode());
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
        }
    }

    private static void validatePrompt(String prompt) {
        if (prompt == null || prompt.isBlank() || prompt.length() > 32000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "prompt must contain 1 to 32000 characters");
        }
    }

    public record CommandRequest(String prompt, String targetNode) { }
    public record BatchRequest(List<String> prompts, String targetNode) { }
}
