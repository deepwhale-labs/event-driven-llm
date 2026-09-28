package io.github.deepwhalelabs.eventdrivenllm;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class CoralClient {
    private static final String NAMESPACE = "event-driven-llm";
    private final ObjectMapper mapper;
    private final CoralHttp http;
    private final URI base;
    private final Path runtime;
    private String sessionId;
    private CoralMcp notifier;

    public CoralClient(ObjectMapper mapper, @Value("${app.coral.base-url}") URI base,
            @Value("${app.coral.runtime-dir}") Path runtime) {
        this.mapper = mapper;
        this.http = new CoralHttp(mapper);
        this.base = base;
        this.runtime = runtime;
    }

    public synchronized Map<String, String> status() {
        state();
        return Map.of("status", "connected", "namespace", NAMESPACE, "sessionId", sessionId);
    }

    public synchronized Optional<Delivery> result(UUID taskId) {
        JsonNode thread = findThread(state(), taskId.toString());
        return thread == null ? Optional.empty() : readDelivery(thread, taskId.toString());
    }

    // One notifier instance per local deployment. This is not a distributed exactly-once guarantee.
    public synchronized void deliver(LlmResult result) {
        String taskId = UUID.fromString(result.taskId()).toString();
        try {
            JsonNode thread = findThread(state(), taskId);
            if (thread != null && readDelivery(thread, taskId).isPresent()) {
                return;
            }
            String threadId;
            if (thread == null) {
                JsonNode receipt = notifier.tool("coral_create_thread", Map.of(
                        "threadName", "task:" + taskId, "participantNames", List.of("observer")));
                threadId = receipt.path("thread").path("id").asText();
                if (threadId.isBlank()) {
                    throw new CoralException("Coral thread receipt missing");
                }
            } else {
                threadId = thread.path("id").asText();
            }
            notifier.tool("coral_send_message", Map.of("threadId", threadId,
                    "content", mapper.writeValueAsString(result), "mentions", List.of("observer")));
        } catch (CoralException ex) {
            notifier = null; // Reinitialize MCP on retry; discover the existing thread before sending again.
            throw ex;
        } catch (IOException ex) {
            throw new CoralException("Coral result serialization failed");
        }
    }

    private JsonNode state() {
        if (sessionId != null) {
            JsonNode current = admin("/session/" + NAMESPACE + "/" + sessionId + "/extended", null, true);
            if (!current.isMissingNode() && current.path("base").path("status").path("type").asText().equals("executed")) {
                connect();
                return current;
            }
            sessionId = null;
            notifier = null;
        }
        JsonNode sessions = admin("/namespace/" + NAMESPACE, null, true);
        if (!sessions.isMissingNode() && !sessions.isArray()) {
            throw new CoralException("Invalid Coral session list");
        }
        for (JsonNode session : sessions) {
            if (session.path("status").path("type").asText().equals("executed")) {
                sessionId = UUID.fromString(session.path("id").asText()).toString();
                break;
            }
        }
        if (sessionId == null) {
            JsonNode receipt = admin("/session", Map.of(
                    "agentGraphRequest", Map.of("agents", List.of(agent("notifier"), agent("observer")),
                            "groups", List.of(List.of("notifier", "observer"))),
                    "namespaceProvider", Map.of("type", "create_if_not_exists", "namespaceRequest",
                            Map.of("name", NAMESPACE, "deleteOnLastSessionExit", false)),
                    "execution", Map.of("mode", "immediate")), false);
            sessionId = UUID.fromString(receipt.path("sessionId").asText()).toString();
        }
        connect();
        return admin("/session/" + NAMESPACE + "/" + sessionId + "/extended", null, false);
    }

    private Map<String, Object> agent(String name) {
        return Map.of("id", Map.of("name", "event-driven-endpoint", "version", "0.1.0",
                        "registrySourceId", Map.of("type", "local")),
                "name", name, "provider", Map.of("type", "local", "runtime", "executable"),
                "options", Map.of(), "blocking", false);
    }

    private void connect() {
        if (notifier == null) {
            CoralMcp candidate = new CoralMcp(http, endpoint("notifier"));
            new CoralMcp(http, endpoint("observer"));
            notifier = candidate;
        }
    }

    private URI endpoint(String name) {
        Path file = runtime.resolve(sessionId + "-" + name + ".url");
        try {
            long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
            while (!Files.isRegularFile(file)) {
                if (System.nanoTime() > deadline) {
                    throw new CoralException("Coral endpoint startup timed out");
                }
                Thread.sleep(100);
            }
            return containerEndpoint(base, Files.readString(file).trim());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new CoralException("Coral endpoint startup interrupted");
        } catch (IOException ex) {
            throw new CoralException("Coral endpoint file unavailable");
        }
    }

    static URI containerEndpoint(URI base, String captured) {
        try {
            URI local = URI.create(captured);
            if (!"http".equals(local.getScheme()) || !"localhost".equals(local.getHost())
                    || local.getPort() != 5555 || local.getUserInfo() != null || local.getFragment() != null
                    || !local.getPath().startsWith("/mcp/")) {
                throw new IllegalArgumentException();
            }
            return URI.create(base.getScheme() + "://" + base.getRawAuthority() + local.getRawPath()
                    + (local.getRawQuery() == null ? "" : "?" + local.getRawQuery()));
        } catch (IllegalArgumentException ex) {
            throw new CoralException("Invalid Coral endpoint");
        }
    }

    private JsonNode admin(String path, Object body, boolean allowMissing) {
        try {
            String key = Files.readString(runtime.resolve("admin-key")).trim();
            if (!key.matches("[a-f0-9]{64}")) {
                throw new CoralException("Invalid Coral admin key file");
            }
            return http.request(base.resolve("/api/v1/local" + path),
                    Map.of("Authorization", "Bearer " + key), body, allowMissing).body();
        } catch (IOException ex) {
            throw new CoralException("Coral admin key file unavailable");
        }
    }

    private JsonNode findThread(JsonNode state, String taskId) {
        for (JsonNode thread : state.path("threads")) {
            if (thread.path("name").asText().equals("task:" + taskId)) {
                return thread;
            }
        }
        return null;
    }

    private Optional<Delivery> readDelivery(JsonNode thread, String taskId) {
        for (JsonNode message : thread.path("messages")) {
            if (!message.path("senderName").asText().equals("notifier")) {
                continue;
            }
            try {
                LlmResult result = mapper.readValue(message.path("text").asText(), LlmResult.class);
                if (taskId.equals(result.taskId())) {
                    return Optional.of(new Delivery(taskId, thread.path("id").asText(), result.nodeId(), result.output()));
                }
            } catch (IOException ex) {
                // Other messages in Coral are not application delivery receipts.
            }
        }
        return Optional.empty();
    }

    public record Delivery(String taskId, String threadId, String nodeId, String output) { }
}
