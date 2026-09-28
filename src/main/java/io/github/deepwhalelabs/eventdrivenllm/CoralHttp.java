package io.github.deepwhalelabs.eventdrivenllm;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;

/** Coral 1.4.0 answers MCP POST requests with JSON (no persistent SSE stream). */
final class CoralHttp {
    private final ObjectMapper mapper;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    CoralHttp(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    Reply request(URI uri, Map<String, String> headers, Object body, boolean allowMissing) {
        try {
            var builder = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30))
                    .header("Accept", "application/json, text/event-stream");
            headers.forEach(builder::header);
            if (body != null) {
                builder.header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)));
            }
            var response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (allowMissing && response.statusCode() == 404) {
                return new Reply(MissingNode.getInstance(), null);
            }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new CoralException("Coral HTTP status " + response.statusCode());
            }
            JsonNode json = response.body().isBlank() ? MissingNode.getInstance() : mapper.readTree(response.body());
            return new Reply(json, response.headers().firstValue("Mcp-Session-Id").orElse(null));
        } catch (CoralException ex) {
            throw ex;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new CoralException("Coral request interrupted");
        } catch (Exception ex) {
            throw new CoralException("Coral request failed");
        }
    }

    record Reply(JsonNode body, String sessionId) { }
}
