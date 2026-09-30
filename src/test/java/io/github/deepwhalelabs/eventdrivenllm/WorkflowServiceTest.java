package io.github.deepwhalelabs.eventdrivenllm;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class WorkflowServiceTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private JdbcTemplate jdbc;
    private TaskStore tasks;
    private WorkflowService workflows;
    private CoralWorkflowClient coral;
    private InferenceService model;
    private LlmCommandConsumer worker;
    private CoralResultConsumer notifier;
    private final String thread = UUID.randomUUID().toString();

    @BeforeEach
    void setup() {
        var data = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000", "sa", "");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(data);
        jdbc = new JdbcTemplate(data);
        var tx = new DataSourceTransactionManager(data);
        var routing = new Routing("commands", "results", "one", "one,two");
        tasks = new TaskStore(jdbc, tx, mapper, routing, 1200000);
        coral = mock(CoralWorkflowClient.class);
        when(coral.exchange(anyString(), anyList(), anyString())).thenAnswer(call ->
                new CoralWorkflowClient.Conversation(thread, call.getArgument(2), List.copyOf(call.getArgument(1))));
        workflows = new WorkflowService(jdbc, tx, tasks, mock(CoralClient.class), coral, mapper, "ollama", "fixture-model");
        model = mock(InferenceService.class);
        worker = new LlmCommandConsumer(model, mapper, tasks, routing, workflows);
        notifier = new CoralResultConsumer(mapper, tasks, workflows);
    }

    private void infer(Task task) throws Exception {
        worker.consume(mapper.writeValueAsString(new LlmCommand(task.taskId(), task.prompt(), task.attempt())));
    }
    private String event(Task task) throws Exception {
        return mapper.writeValueAsString(new LlmResult(task.taskId(), task.nodeId(), task.output(), task.attempt()));
    }
    private WorkflowService.Step last(String id) { return workflows.get(id).steps().getLast(); }
    private void deliverLast(String id) throws Exception { notifier.consume(event(last(id).task())); }

    @Test
    void followsThreeDurableStepsAndUsesReadBackReviewInTheFinalPrompt() throws Exception {
        when(model.generate(anyString(), anyString())).thenReturn("오전 3시입니다.", "오전을 오후로 고치세요.", "오후 3시입니다.");
        var flow = workflows.create("오후 3시 회의를 안내하세요.", null, null);
        for (int i = 0; i < 3; i++) { infer(last(flow.workflowId()).task()); deliverLast(flow.workflowId()); }
        var completed = workflows.get(flow.workflowId());
        assertThat(completed.status()).isEqualTo("SUCCEEDED");
        assertThat(completed.steps()).extracting(WorkflowService.Step::stage).containsExactly("DRAFT", "REVIEW", "REVISION");
        assertThat(completed.inferenceCalls()).isEqualTo(3);
        assertThat(completed.steps()).allSatisfy(s -> assertThat(s.task().threadId()).isEqualTo(thread));
        var revision = completed.steps().getLast();
        assertThat(revision.task().prompt()).contains("오전을 오후로 고치세요.", "오전 3시입니다.", "오후 3시 회의를 안내하세요.");
        assertThat(revision.sourceReader()).isEqualTo("writer");
        assertThat(revision.systemPrompt()).contains("최종 작성자");
        assertThat(revision.sourceSnapshot()).contains("오전을 오후로 고치세요.");
        assertThat(completed.steps().get(1).sourceReader()).isEqualTo("reviewer");
        for (var step : completed.steps()) notifier.consume(event(step.task()));
        assertThat(workflows.get(flow.workflowId()).steps()).hasSize(3);
        verify(model, times(3)).generate(anyString(), anyString());
        verify(coral, times(3)).exchange(anyString(), anyList(), anyString());
    }

    @Test
    void unavailableCoralKeepsTheSavedDraftAndRetriesWithoutAnotherModelCall() throws Exception {
        when(model.generate(anyString(), anyString())).thenReturn("saved draft");
        var flow = workflows.create("request", null, null);
        infer(last(flow.workflowId()).task());
        when(coral.exchange(anyString(), anyList(), anyString())).thenThrow(new CoralException("read unavailable"));
        assertThatThrownBy(() -> deliverLast(flow.workflowId())).isInstanceOf(CoralException.class);
        assertThat(last(flow.workflowId()).task().status()).isEqualTo("DELIVERING");
        assertThat(workflows.get(flow.workflowId()).steps()).hasSize(1);
        Task draft = last(flow.workflowId()).task();
        tasks.fail(draft.taskId(), draft.attempt(), "DELIVERY");
        assertThat(workflows.get(flow.workflowId()).status()).isEqualTo("FAILED");
        workflows.retry(flow.workflowId());
        doAnswer(call -> new CoralWorkflowClient.Conversation(thread, call.getArgument(2), List.copyOf(call.getArgument(1))))
                .when(coral).exchange(anyString(), anyList(), anyString());
        deliverLast(flow.workflowId());
        assertThat(last(flow.workflowId()).stage()).isEqualTo("REVIEW");
        assertThat(last(flow.workflowId()).task().prompt()).contains("saved draft");
        verify(model, times(1)).generate(anyString(), anyString());
    }

    @Test
    void providedDraftSkipsGenerationAndDuplicateDeliveryOnlyCreatesOneNextStep() throws Exception {
        var flow = workflows.create("classify the input", "one", "incorrect label");
        Task draft = last(flow.workflowId()).task();
        assertThat(draft.status()).isEqualTo("DELIVERING");
        assertThat(workflows.get(flow.workflowId()).inferenceCalls()).isZero();
        String payload = event(draft);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> { notifier.consume(payload); return true; });
            var b = executor.submit(() -> { notifier.consume(payload); return true; });
            assertThat(a.get()).isTrue();
            assertThat(b.get()).isTrue();
        }
        assertThat(workflows.get(flow.workflowId()).steps()).hasSize(2);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM tasks", Integer.class)).isEqualTo(2);
        verifyNoInteractions(model);
        verify(coral, times(1)).exchange(anyString(), anyList(), anyString());
    }

    @Test
    void failedReviewRetriesOnlyReviewAndOldResultsCannotAdvanceTheFlow() throws Exception {
        var flow = workflows.create("request", null, "draft");
        deliverLast(flow.workflowId());
        Task review = last(flow.workflowId()).task();
        when(model.generate(anyString(), anyString())).thenThrow(new IllegalStateException("model unavailable")).thenReturn("feedback");
        assertThatThrownBy(() -> infer(review)).isInstanceOf(IllegalStateException.class);
        tasks.fail(review.taskId(), 1, "COMMAND");
        workflows.retry(flow.workflowId());
        notifier.consume(mapper.writeValueAsString(new LlmResult(review.taskId(), "one", "stale", 1)));
        assertThat(workflows.get(flow.workflowId()).steps()).hasSize(2);
        infer(last(flow.workflowId()).task());
        deliverLast(flow.workflowId());
        assertThat(last(flow.workflowId()).stage()).isEqualTo("REVISION");
        assertThat(workflows.get(flow.workflowId()).inferenceCalls()).isEqualTo(2);
        assertThat(workflows.get(flow.workflowId()).steps().getFirst().task().attempt()).isEqualTo(1);
    }

    @Test
    void apiValidatesRequestsRollsBackInvalidRoutingAndRequiresAuthentication() throws Exception {
        var mvc = MockMvcBuilders.standaloneSetup(new WorkflowController(workflows)).addFilters(new ApiKeyFilter("access")).build();
        mvc.perform(post("/api/workflows").servletPath("/api/workflows").contentType("application/json").content("{\"prompt\":\"hello\"}"))
                .andExpect(status().isUnauthorized());
        for (String body : List.of("{}", "{\"prompt\":\"hello\",\"initialDraft\":\" \"}",
                "{\"prompt\":\"hello\",\"targetNode\":\"missing\"}", mapper.writeValueAsString(java.util.Map.of("prompt", "a".repeat(8001))))) {
            mvc.perform(post("/api/workflows").header("X-API-Key", "access").contentType("application/json").content(body)).andExpect(status().isBadRequest());
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM workflows", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM tasks", Integer.class)).isZero();
        mvc.perform(post("/api/workflows").header("X-API-Key", "access").contentType("application/json").content("{\"prompt\":\"hello\",\"initialDraft\":\"draft\"}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.steps[0].provided").value(true));
        mvc.perform(get("/api/workflows").header("X-API-Key", "access")).andExpect(status().isOk()).andExpect(jsonPath("$[0].inferenceCalls").value(0));
        mvc.perform(get("/api/workflows?limit=51").header("X-API-Key", "access")).andExpect(status().isBadRequest());
    }
}
