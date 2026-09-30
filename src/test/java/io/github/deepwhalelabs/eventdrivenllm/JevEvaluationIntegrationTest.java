package io.github.deepwhalelabs.eventdrivenllm;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:jev-integration;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa", "spring.datasource.password=", "spring.ai.model.chat=none",
        "spring.kafka.listener.auto-startup=false", "spring.kafka.admin.auto-create=false", "app.jobs-enabled=false",
        "app.jev.mode=shadow", "app.jev.api-key=test-only-key"
})
class JevEvaluationIntegrationTest {
    private static final HttpServer SERVER = startServer();
    private static final AtomicInteger CALLS = new AtomicInteger();
    @Autowired TaskStore tasks;
    @Autowired EvaluationStore evaluations;
    @Autowired JevResultConsumer evaluator;
    @Autowired LlmCommandConsumer worker;
    @Autowired CoralResultConsumer notifier;
    @Autowired JevClient client;
    @Autowired ObjectMapper mapper;
    @MockitoBean CoralClient coral;

    private static HttpServer startServer() {
        try {
            var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/systemone", exchange -> {
                CALLS.incrementAndGet();
                exchange.getRequestBody().readAllBytes();
                String check = "{\"type\":\"choice\",\"choice\":\"fail\",\"confidence\":0.96,\"probabilities\":{\"pass\":0.02,\"fail\":0.97,\"unknown\":0.01}}";
                byte[] bytes = ("{\"model\":\"jev-1.13.0\",\"answers\":{\"fulfillment\":" + check + ",\"faithfulness\":" + check + ",\"constraints\":" + check + "}}").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
            return server;
        } catch (Exception ex) { throw new ExceptionInInitializerError(ex); }
    }

    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("app.jev.endpoint", () -> "http://127.0.0.1:" + SERVER.getAddress().getPort() + "/v1/systemone");
    }
    @AfterAll static void stop() { SERVER.stop(0); }

    @Test
    void shadowEvaluationPersistsARealHttpFixtureWithoutBlockingDeliveryAndProtectsItsApi() throws Exception {
        Task created = tasks.create("안녕하세요", null);
        worker.consume(mapper.writeValueAsString(new LlmCommand(created.taskId(), created.prompt(), 1)));
        Task generated = tasks.get(created.taskId());
        String result = mapper.writeValueAsString(new LlmResult(created.taskId(), generated.nodeId(), generated.output(), 1));
        evaluator.consume(result);
        evaluator.consume(result);
        assertThat(CALLS.get()).isZero(); // Kafka ingestion only persists a job.
        when(coral.deliver(any())).thenReturn(new CoralClient.Delivery(created.taskId(), UUID.randomUUID().toString(), generated.nodeId(), generated.output()));
        notifier.consume(result);
        assertThat(tasks.get(created.taskId()).status()).isEqualTo("SUCCEEDED");
        new EvaluationJobs(evaluations, client).evaluateNext();
        assertThat(CALLS.get()).isEqualTo(1);
        assertThat(evaluations.list(created.taskId()).getFirst().report().verdict()).isEqualTo("REJECT");
        assertThat(tasks.get(created.taskId()).status()).isEqualTo("SUCCEEDED");
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new EvaluationController(evaluations, client))
                .addFilters(new ApiKeyFilter("access-key")).build();
        String path = "/api/tasks/" + created.taskId() + "/evaluations";
        mvc.perform(get(path).servletPath(path)).andExpect(status().isUnauthorized());
        mvc.perform(post(path).servletPath(path)).andExpect(status().isUnauthorized());
        mvc.perform(get(path).header("X-API-Key", "access-key")).andExpect(status().isOk())
                .andExpect(jsonPath("$[0].report.verdict").value("REJECT")).andExpect(jsonPath("$[0].claimToken").doesNotExist())
                .andExpect(jsonPath("$[0].output").doesNotExist());
        mvc.perform(get("/api/evaluations/config").servletPath("/api/evaluations/config")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/evaluations/config").header("X-API-Key", "access-key")).andExpect(status().isOk())
                .andExpect(jsonPath("$.mode").value("SHADOW")).andExpect(jsonPath("$.apiKey").doesNotExist());
        mvc.perform(post(path).header("X-API-Key", "access-key")).andExpect(status().isAccepted());
        assertThat(evaluations.list(created.taskId())).hasSize(2);
    }
}
