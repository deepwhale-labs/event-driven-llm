package io.github.deepwhalelabs.eventdrivenllm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class CodexCliClientTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final InferenceConfig config = new InferenceConfig("codex-cli", "unused", 256, "fixture-cli");
    private final String token = "x".repeat(64);
    private HttpServer server;
    private String url;

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        url = "http://127.0.0.1:" + server.getAddress().getPort();
        server.start();
    }
    @AfterEach void stop() { server.stop(0); }

    @Test
    void sendsRoleAndUnicodeInputToAuthenticatedBridgeAndReturnsOnlyFinalAnswer() {
        var received = new AtomicReference<String>();
        var auth = new AtomicReference<String>();
        server.createContext("/generate", exchange -> {
            received.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            auth.set(exchange.getRequestHeaders().getFirst("X-CLI-Token"));
            byte[] body = "{\"model\":\"fixture-cli\",\"output\":\"부정\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        assertThat(new CodexCliClient(mapper, config, url, token).generate("검토자", "고장 난 제품")).isEqualTo("부정");
        assertThat(auth.get()).isEqualTo(token);
        assertThat(received.get()).contains("검토자", "고장 난 제품", "fixture-cli").doesNotContain(token);
    }

    @Test
    void rejectsModelMismatchEmptyAnswerErrorsAndConnectionFailure() {
        var status = new java.util.concurrent.atomic.AtomicInteger(200);
        var payload = new AtomicReference<>("{\"model\":\"other\",\"output\":\"answer\"}");
        server.createContext("/generate", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] body = payload.get().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.get(), body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        var client = new CodexCliClient(mapper, config, url, token);
        assertThatThrownBy(() -> client.generate(null, "test")).hasMessageContaining("model does not match");
        payload.set("{\"model\":\"fixture-cli\",\"output\":\" \"}");
        assertThatThrownBy(() -> client.generate(null, "test")).hasMessageContaining("invalid final answer");
        for (int code : new int[]{401, 503, 504}) {
            status.set(code);
            assertThatThrownBy(() -> client.generate(null, "test")).hasMessageContaining("HTTP " + code).hasMessageNotContaining(token);
        }
        server.stop(0);
        assertThatThrownBy(() -> client.generate(null, "test")).hasMessageContaining("Cannot reach Codex CLI bridge");
    }

    @Test
    void cliRequiresModelLocalEndpointAndBridgeCredential() {
        assertThatThrownBy(() -> new InferenceConfig("codex-cli", "unused", 256, "")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CodexCliClient(mapper, config, url, "")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CodexCliClient(mapper, config, "https://example.com", token)).isInstanceOf(IllegalArgumentException.class);
    }
}
