package io.github.deepwhalelabs.eventdrivenllm;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RuntimeController {
    private final RuntimeInfo info;

    public RuntimeController(@Value("${spring.ai.model.chat}") String mode,
            @Value("${spring.ai.ollama.chat.options.model}") String model,
            @Value("${spring.ai.ollama.chat.options.num-predict:512}") int maxTokens) {
        this.info = new RuntimeInfo(mode.equals("none") ? "DEMO" : "OLLAMA", mode.equals("none") ? null : model,
                mode.equals("none") ? null : maxTokens, CommandController.MAX_BATCH_SIZE, CommandController.MAX_BATCH_CHARACTERS);
    }

    @GetMapping("/api/runtime")
    public RuntimeInfo runtime() { return info; }

    public record RuntimeInfo(String mode, String model, Integer maxOutputTokens, int maxBatchSize, int maxBatchCharacters) { }
}
