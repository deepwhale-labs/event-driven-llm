package io.github.deepwhalelabs.eventdrivenllm;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class JevClientTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpServer server;
    private URI endpoint;
    private String body;
    private int status;
    private String received;
    private String authorization;
    private final AtomicInteger calls = new AtomicInteger();

    @BeforeEach
    void start() throws Exception {
        status = 200;
        body = response("pass", .95);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/systemone", exchange -> {
            calls.incrementAndGet();
            received = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            authorization = exchange.getRequestHeaders().getFirst("Authorization");
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Retry-After", "12");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/v1/systemone");
    }

    @AfterEach void stop() { server.stop(0); }

    private JevClient client() { return new JevClient(mapper, "shadow", "secret-test-key", endpoint, "jev-1.13.0", .85, 2000); }
    static Evaluation job(String prompt, String output) {
        return new Evaluation("id", "task", 1, "generation", "MANUAL", "RUNNING", 1,
                "jev-1.13.0", JevClient.RUBRIC_VERSION, .85, prompt, output, "claim", null, 1, 2L, null, 1, null);
    }
    String response(String choice, double confidence) throws Exception {
        var answer = Map.of("type", "choice", "choice", choice, "confidence", confidence,
                "probabilities", Map.of("pass", choice.equals("pass") ? .9 : .05,
                        "fail", choice.equals("fail") ? .9 : .05, "unknown", choice.equals("unknown") ? .9 : .05));
        return mapper.writeValueAsString(Map.of("model", "jev-1.13.0", "answers",
                Map.of("fulfillment", answer, "faithfulness", answer, "constraints", answer), "usage", Map.of("input_tokens", 200, "output_tokens", 30)));
    }

    @Test
    void sendsOfficialTypedQuestionsAndKeepsCredentialsOutOfConfiguration() throws Exception {
        var client = client();
        var report = client.evaluate(job("오후 3시로 변경됐다고 알려주세요.", "회의는 오후 3시입니다."));
        assertThat(report.verdict()).isEqualTo("PASS");
        assertThat(report.checks()).hasSize(3);
        assertThat(authorization).isEqualTo("Bearer secret-test-key");
        var request = mapper.readTree(received);
        assertThat(request.path("model").asText()).isEqualTo("jev-1.13.0");
        assertThat(request.path("state").path("original_request").asText()).contains("오후");
        assertThat(request.path("state").path("candidate_response").asText()).isEqualTo("회의는 오후 3시입니다.");
        assertThat(request.path("questions").size()).isEqualTo(3);
        assertThat(request.path("questions").path("faithfulness").path("criteria").has("unknown")).isTrue();
        assertThat(mapper.writeValueAsString(client.configuration())).doesNotContain("secret-test-key", endpoint.toString());
    }

    @Test
    void verdictUsesEveryCriterionAndUncertaintyRatherThanJustTheWinningOption() throws Exception {
        body = response("fail", .96);
        assertThat(client().evaluate(job("request", "answer")).verdict()).isEqualTo("REJECT");
        body = response("fail", .6);
        assertThat(client().evaluate(job("request", "answer")).verdict()).isEqualTo("REVIEW");
        body = response("unknown", .99);
        assertThat(client().evaluate(job("request", "answer")).verdict()).isEqualTo("REVIEW");
        body = response("pass", .8);
        assertThat(client().evaluate(job("request", "answer")).verdict()).isEqualTo("REVIEW");
        var mixed = mapper.readTree(response("pass", .95));
        ((com.fasterxml.jackson.databind.node.ObjectNode) mixed.path("answers")).set("constraints", mapper.readTree(response("fail", .95)).path("answers").path("constraints"));
        body = mapper.writeValueAsString(mixed);
        assertThat(client().evaluate(job("request", "answer")).verdict()).isEqualTo("REJECT");
    }

    @Test
    void rejectsMissingWrongTypedOrInvalidProbabilityFieldsInsteadOfApproving() throws Exception {
        for (String invalid : new String[]{"null", "{}", "not-json", response("pass", .95).replace("0.95", "2.0"),
                response("pass", .95).replace("0.9", "0.2"), response("pass", .95).replace("\"choice\":\"pass\"", "\"choice\":\"other\""),
                response("pass", .95).replace("\"type\":\"choice\"", "\"type\":\"noul\"")}) {
            body = invalid;
            assertThatThrownBy(() -> client().evaluate(job("request", "answer")))
                    .isInstanceOf(JevClient.EvaluationException.class).hasMessage("INVALID_RESPONSE");
        }
    }

    @Test
    void distinguishesRetryableFailuresAndNeverCopiesTheProviderErrorBody() {
        body = "secret-test-key and private prompt echoed by provider";
        status = 401;
        var unauthorized = catchThrowableOfType(() -> client().evaluate(job("request", "answer")), JevClient.EvaluationException.class);
        assertThat(unauthorized.getMessage()).isEqualTo("HTTP_401");
        assertThat(unauthorized.retryable).isFalse();
        status = 429;
        var limited = catchThrowableOfType(() -> client().evaluate(job("request", "answer")), JevClient.EvaluationException.class);
        assertThat(limited.retryable).isTrue();
        assertThat(limited.retryAfterMillis).isEqualTo(12000);
    }

    @Test
    void noKeyDisabledAndOversizedRequestsNeverContactTheProvider() {
        var disabled = new JevClient(mapper, "off", "key", endpoint, "jev-1.13.0", .85, 2000);
        var noKey = new JevClient(mapper, "shadow", "", endpoint, "jev-1.13.0", .85, 2000);
        assertThatThrownBy(() -> disabled.evaluate(job("request", "answer"))).hasMessage("NOT_CONFIGURED");
        assertThatThrownBy(() -> noKey.evaluate(job("request", "answer"))).hasMessage("NOT_CONFIGURED");
        assertThatThrownBy(() -> client().evaluate(job("x".repeat(24000), "answer"))).hasMessage("INPUT_TOO_LARGE");
        assertThat(calls.get()).isZero();
    }
}
