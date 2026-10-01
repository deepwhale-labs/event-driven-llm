package io.github.deepwhalelabs.eventdrivenllm;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CodexCliClient {
    private final ObjectMapper mapper;
    private final InferenceConfig config;
    private final URI endpoint;
    private final String token;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    public CodexCliClient(ObjectMapper mapper, InferenceConfig config,
            @Value("${app.inference.cli.url}") String url,
            @Value("${app.inference.cli.token}") String token) {
        this.mapper = mapper;
        this.config = config;
        this.endpoint = URI.create(url + "/generate");
        this.token = token;
        if (config.provider().equals("codex-cli")) {
            if (token.length() < 32) throw new IllegalArgumentException("CLI_BRIDGE_TOKEN must contain at least 32 characters");
            if (!endpoint.getScheme().equals("http") || !Set.of("localhost", "127.0.0.1", "host.docker.internal", "[::1]").contains(endpoint.getHost())
                    || endpoint.getUserInfo() != null || endpoint.getQuery() != null || endpoint.getFragment() != null) {
                throw new IllegalArgumentException("CLI_BRIDGE_URL must address the local CLI bridge");
            }
        }
    }

    public String generate(String system, String prompt) {
        try {
            var body = mapper.writeValueAsString(Map.of("model", config.cliModel(), "system", system == null ? "" : system, "prompt", prompt));
            var request = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(330))
                    .header("Content-Type", "application/json; charset=utf-8").header("X-CLI-Token", token)
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build();
            var response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) throw new IllegalStateException("Codex CLI bridge failed (HTTP " + response.statusCode() + "); check bridge logs and CLI login");
            var result = mapper.readTree(response.body());
            if (!config.cliModel().equals(result.path("model").asText())) throw new IllegalStateException("Codex CLI model does not match app configuration");
            var output = result.path("output");
            if (!output.isTextual() || output.asText().isBlank() || output.asText().length() > 128000) {
                throw new IllegalStateException("Codex CLI returned an invalid final answer");
            }
            return output.asText();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Codex CLI request interrupted", ex);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot reach Codex CLI bridge; start scripts/start-codex.ps1", ex);
        }
    }
}
