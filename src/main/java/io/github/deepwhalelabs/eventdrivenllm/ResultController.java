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

    public ResultController(CoralClient coral) {
        this.coral = coral;
    }

    @GetMapping("/api/coral")
    public Map<String, String> status() {
        return coral.status();
    }

    @GetMapping("/api/results/{taskId}")
    public CoralClient.Delivery result(@PathVariable UUID taskId) {
        return coral.result(taskId).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "No result in the current Coral session"));
    }

    @ExceptionHandler(CoralException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> unavailable() {
        return Map.of("error", "Coral unavailable");
    }
}
