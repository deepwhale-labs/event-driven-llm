package io.github.deepwhalelabs.eventdrivenllm;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RuntimeController {
    private final RuntimeInfo info;

    public RuntimeController(InferenceConfig config) {
        this.info = new RuntimeInfo(config.mode(), config.provider().equals("none") ? null : config.model(),
                config.maxOutputTokens(), CommandController.MAX_BATCH_SIZE, CommandController.MAX_BATCH_CHARACTERS);
    }

    @GetMapping("/api/runtime")
    public RuntimeInfo runtime() { return info; }

    public record RuntimeInfo(String mode, String model, Integer maxOutputTokens, int maxBatchSize, int maxBatchCharacters) { }
}
