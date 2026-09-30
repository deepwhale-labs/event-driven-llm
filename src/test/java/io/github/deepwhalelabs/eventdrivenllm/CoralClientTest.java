package io.github.deepwhalelabs.eventdrivenllm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoralClientTest {
    @TempDir Path runtime;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ArrayNode threads = mapper.createArrayNode();
    private HttpServer server;
    private URI base;
    private CoralClient client;
    private String sessionId;
    private int createdSessions;
    private int sentMessages;
    private boolean failAfterSend;
    private boolean remoteFailure;
    private boolean resourceFailure;
    private final List<String> readers = new ArrayList<>();

    @BeforeEach
    void start() throws IOException {
        Files.writeString(runtime.resolve("admin-key"), "a".repeat(64));
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        client = new CoralClient(mapper, base, runtime);
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void deliversAndFindsResultWithoutDuplicatingAfterAppRestart() {
        var result = new LlmResult(UUID.randomUUID().toString(), "worker", "hello");
        client.deliver(result);
        var restarted = new CoralClient(mapper, base, runtime);
        restarted.deliver(result);
        assertThat(restarted.result(UUID.fromString(result.taskId())).orElseThrow().output()).isEqualTo("hello");
        assertThat(createdSessions).isEqualTo(1);
        assertThat(threads.size()).isEqualTo(1);
        assertThat(sentMessages).isEqualTo(1);
    }

    @Test
    void retryAfterLostSendResponseChecksRemoteReceiptFirst() {
        failAfterSend = true;
        var result = new LlmResult(UUID.randomUUID().toString(), "worker", "hello");
        assertThatThrownBy(() -> client.deliver(result)).isInstanceOf(CoralException.class);
        client.deliver(result);
        assertThat(sentMessages).isEqualTo(1);
        assertThat(threads.size()).isEqualTo(1);
    }

    @Test
    void createsNewSessionAfterCoralLosesItsInMemoryState() {
        String previous = client.status().get("sessionId");
        sessionId = null;
        threads.removeAll();
        assertThat(client.status().get("sessionId")).isNotEqualTo(previous);
        assertThat(createdSessions).isEqualTo(2);
    }

    @Test
    void remoteErrorsDoNotLeakCredentialsOrBodies() {
        remoteFailure = true;
        assertThatThrownBy(client::status).isInstanceOf(CoralException.class)
                .hasMessage("Coral HTTP status 503").hasNoCause();
    }

    @Test
    void onlyRewritesLocalCoralEndpoints() {
        assertThat(CoralClient.containerEndpoint(base, "http://localhost:5555/mcp/v1/secret/mcp?q=a%2Fb"))
                .isEqualTo(base.resolve("/mcp/v1/secret/mcp?q=a%2Fb"));
        for (String url : List.of("http://evil.example:5555/mcp/secret", "http://localhost:9999/mcp/secret",
                "http://localhost:5555/admin", "http://secret@localhost:5555/mcp/secret", "not a secret URL")) {
            assertThatThrownBy(() -> CoralClient.containerEndpoint(base, url))
                    .isInstanceOf(CoralException.class).hasMessage("Invalid Coral endpoint").hasNoCause();
        }
    }

    @Test
    void workflowRolesReadTheirMcpResourcesAndRestoreConversationAfterCoralRestart() {
        var workflow = new CoralWorkflowClient(mapper, base, runtime);
        String id = UUID.randomUUID().toString();
        var draft = new CoralWorkflowClient.Message(id, "draft-task", "DRAFT", "오후 3시 안내", "오전 3시 안내");
        var review = new CoralWorkflowClient.Message(id, "review-task", "REVIEW", "오후 3시 안내", "오전을 오후로 고치세요");
        var first = workflow.exchange(id, List.of(draft), "reviewer");
        var second = workflow.exchange(id, List.of(draft, review), "writer");
        assertThat(second.messages()).containsExactly(draft, review);
        assertThat(second.threadId()).isEqualTo(first.threadId());
        assertThat(readers).containsExactly("reviewer", "writer");
        assertThat(sentMessages).isEqualTo(2);
        var restarted = new CoralWorkflowClient(mapper, base, runtime);
        restarted.exchange(id, List.of(draft, review), "writer");
        assertThat(sentMessages).isEqualTo(2);
        sessionId = null;
        threads.removeAll();
        var restored = restarted.exchange(id, List.of(draft, review), "writer");
        assertThat(restored.threadId()).isNotEqualTo(first.threadId());
        assertThat(restored.messages()).containsExactly(draft, review);
        assertThat(sentMessages).isEqualTo(4);
    }

    @Test
    void workflowNeverSubstitutesDbContextForAnUnavailableCoralReadAndDeduplicatesLostSend() {
        var workflow = new CoralWorkflowClient(mapper, base, runtime);
        String id = UUID.randomUUID().toString();
        var draft = new CoralWorkflowClient.Message(id, "draft", "DRAFT", "request", "answer");
        failAfterSend = true;
        assertThatThrownBy(() -> workflow.exchange(id, List.of(draft), "reviewer")).isInstanceOf(CoralException.class);
        resourceFailure = true;
        assertThatThrownBy(() -> workflow.exchange(id, List.of(draft), "reviewer")).isInstanceOf(CoralException.class);
        resourceFailure = false;
        assertThat(workflow.exchange(id, List.of(draft), "reviewer").messages()).containsExactly(draft);
        assertThat(sentMessages).isEqualTo(1);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            if (remoteFailure) {
                respond(exchange, 503, Map.of("secret", "must-not-appear-in-errors"));
                return;
            }
            if (path.startsWith("/api/")) {
                if (!("Bearer " + "a".repeat(64)).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                    respond(exchange, 403, Map.of());
                    return;
                }
                if (path.endsWith("/local/session") && exchange.getRequestMethod().equals("POST")) {
                    JsonNode creation = mapper.readTree(exchange.getRequestBody());
                    sessionId = UUID.randomUUID().toString();
                    createdSessions++;
                    for (JsonNode agent : creation.path("agentGraphRequest").path("agents")) {
                        String name = agent.path("name").asText();
                        Files.writeString(runtime.resolve(sessionId + "-" + name + ".url"),
                                "http://localhost:5555/mcp/v1/" + name + "/mcp");
                    }
                    respond(exchange, 200, Map.of("sessionId", sessionId));
                } else if (path.contains("/namespace/")) {
                    respond(exchange, sessionId == null ? 404 : 200,
                            sessionId == null ? Map.of() : List.of(session()));
                } else if (sessionId != null && path.contains(sessionId)) {
                    respond(exchange, 200, Map.of("base", session(), "threads", threads));
                } else {
                    respond(exchange, 404, Map.of());
                }
                return;
            }
            JsonNode request = mapper.readTree(exchange.getRequestBody());
            String method = request.path("method").asText();
            if (method.equals("notifications/initialized")) {
                exchange.sendResponseHeaders(202, -1);
                return;
            }
            Object result;
            if (method.equals("initialize")) {
                exchange.getResponseHeaders().add("Mcp-Session-Id", "test-session");
                result = Map.of("protocolVersion", "2025-06-18");
            } else if (method.equals("resources/read")) {
                if (resourceFailure) { respond(exchange, 503, Map.of()); return; }
                String reader = path.split("/")[3];
                readers.add(reader);
                var state = mapper.createArrayNode();
                for (JsonNode thread : threads) {
                    var item = state.addObject().put("threadId", thread.path("id").asText()).put("threadName", thread.path("name").asText());
                    var messages = item.putArray("messages");
                    for (JsonNode message : thread.path("messages")) messages.addObject()
                            .put("messageText", message.path("text").asText()).put("sendingAgentName", message.path("senderName").asText());
                }
                result = Map.of("contents", List.of(Map.of("uri", "coral://state", "text",
                        "# Agents\n```json\n[]\n```\n# Threads and messages\n```json\n" + state + "\n```\n")));
            } else {
                if (!"test-session".equals(exchange.getRequestHeaders().getFirst("Mcp-Session-Id"))) {
                    respond(exchange, 400, Map.of());
                    return;
                }
                JsonNode arguments = request.path("params").path("arguments");
                if (request.path("params").path("name").asText().equals("coral_create_thread")) {
                    var thread = threads.addObject();
                    thread.put("id", UUID.randomUUID().toString());
                    thread.put("name", arguments.path("threadName").asText());
                    thread.putArray("messages");
                    result = Map.of("structuredContent", Map.of("thread", thread));
                } else {
                    sentMessages++;
                    for (JsonNode thread : threads) if (thread.path("id").asText().equals(arguments.path("threadId").asText())) {
                        ((ArrayNode) thread.get("messages")).addObject()
                                .put("senderName", path.split("/")[3]).put("text", arguments.path("content").asText());
                    }
                    if (failAfterSend) {
                        failAfterSend = false;
                        respond(exchange, 502, Map.of("error", "response lost after save"));
                        return;
                    }
                    result = Map.of("structuredContent", Map.of("status", "Message sent successfully"));
                }
            }
            respond(exchange, 200, Map.of("jsonrpc", "2.0", "id", request.get("id"), "result", result));
        }
    }

    private Object session() {
        return Map.of("id", sessionId, "status", Map.of("type", "executed"));
    }

    private void respond(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
}
