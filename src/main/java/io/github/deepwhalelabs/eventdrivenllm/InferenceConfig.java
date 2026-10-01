package io.github.deepwhalelabs.eventdrivenllm;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public record InferenceConfig(String provider, String ollamaModel, int ollamaTokens, String cliModel) {
    public InferenceConfig(@Value("${app.inference.provider}") String provider,
            @Value("${spring.ai.ollama.chat.options.model}") String ollamaModel,
            @Value("${spring.ai.ollama.chat.options.num-predict:512}") int ollamaTokens,
            @Value("${app.inference.cli.model}") String cliModel) {
        if (!java.util.Set.of("none", "ollama", "codex-cli").contains(provider)) {
            throw new IllegalArgumentException("INFERENCE_PROVIDER must be none, ollama or codex-cli");
        }
        if (provider.equals("codex-cli") && (cliModel == null || cliModel.isBlank())) {
            throw new IllegalArgumentException("CLI_MODEL is required for codex-cli");
        }
        this.provider = provider;
        this.ollamaModel = ollamaModel;
        this.ollamaTokens = ollamaTokens;
        this.cliModel = cliModel;
    }

    public String mode() { return switch (provider) { case "none" -> "DEMO"; case "ollama" -> "OLLAMA"; default -> "CODEX_CLI"; }; }
    public String model() { return switch (provider) { case "none" -> "DEMO"; case "ollama" -> ollamaModel; default -> "Codex CLI / " + cliModel; }; }
    public Integer maxOutputTokens() { return provider.equals("ollama") ? ollamaTokens : null; }
}
