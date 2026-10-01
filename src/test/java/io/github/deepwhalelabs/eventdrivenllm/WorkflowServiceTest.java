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
    private ExperimentService experiments;
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
        experiments = new ExperimentService(jdbc, tx, workflows);
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

    @Test
    void experimentSharesTheDraftButIsolatesReviewAndDoesNotLeakTheExpectedAnswer() throws Exception {
        var experiment = experiments.create("request", "same draft", "expected answer", "one");
        String directId = experiment.direct().workflow().workflowId(), reviewId = experiment.review().workflow().workflowId();
        assertThat(experiment.draftMatchesExpected()).isFalse();
        assertThat(experiment.direct().matchesExpected()).isNull();
        when(model.generate(anyString(), anyString())).thenReturn("expected answer", "review-only feedback", "still wrong");
        for (String id : List.of(directId, reviewId)) {
            deliverLast(id);
            String firstStage = event(workflows.get(id).steps().getFirst().task());
            notifier.consume(firstStage);
            while (!workflows.get(id).status().equals("SUCCEEDED")) { infer(last(id).task()); deliverLast(id); }
        }
        var result = experiments.get(experiment.experimentId());
        assertThat(result.status()).isEqualTo("SUCCEEDED");
        assertThat(result.direct().matchesExpected()).isTrue();
        assertThat(result.review().matchesExpected()).isFalse();
        assertThat(result.direct().workflow().steps()).extracting(WorkflowService.Step::stage).containsExactly("DRAFT", "REVISION");
        assertThat(result.direct().workflow().inferenceCalls()).isEqualTo(1);
        assertThat(result.review().workflow().inferenceCalls()).isEqualTo(2);
        assertThat(result.direct().elapsedMillis()).isNotNull();
        var direct = last(directId);
        var review = last(reviewId);
        assertThat(direct.sourceReader()).isEqualTo("writer");
        assertThat(direct.task().prompt()).contains("request", "same draft").doesNotContain("review-only feedback", "expected answer");
        assertThat(review.task().prompt()).contains("review-only feedback").doesNotContain("expected answer");
        assertThat(direct.systemPrompt()).isEqualTo(review.systemPrompt());
        assertThat(result.direct().workflow().steps().getFirst().task().output()).isEqualTo(result.review().workflow().steps().getFirst().task().output());
        verify(coral, atLeastOnce()).exchange(eq(directId), anyList(), eq("writer"));
        verify(coral, atLeastOnce()).exchange(eq(reviewId), anyList(), eq("reviewer"));
        assertThat(experiments.list(10)).hasSize(1);
    }

    @Test
    void experimentDeliveryRetryDoesNotRegenerateEitherAnswerAndUnscoredIsNotAMatch() throws Exception {
        var experiment = experiments.create("request", "draft", null, null);
        String directId = experiment.direct().workflow().workflowId(), reviewId = experiment.review().workflow().workflowId();
        when(model.generate(anyString(), anyString())).thenReturn("answer");
        deliverLast(directId);
        infer(last(directId).task());
        doThrow(new CoralException("offline")).when(coral).exchange(eq(directId), anyList(), anyString());
        assertThatThrownBy(() -> deliverLast(directId)).isInstanceOf(CoralException.class);
        tasks.fail(last(directId).task().taskId(), 1, "DELIVERY");
        deliverLast(reviewId);
        for (int i = 0; i < 2; i++) { infer(last(reviewId).task()); deliverLast(reviewId); }
        assertThat(experiments.get(experiment.experimentId()).status()).isEqualTo("FAILED");
        assertThat(experiments.get(experiment.experimentId()).direct().output()).isEqualTo("answer");
        workflows.retry(directId);
        doAnswer(call -> new CoralWorkflowClient.Conversation(thread, call.getArgument(2), List.copyOf(call.getArgument(1))))
                .when(coral).exchange(eq(directId), anyList(), anyString());
        deliverLast(directId);
        var complete = experiments.get(experiment.experimentId());
        assertThat(complete.status()).isEqualTo("SUCCEEDED");
        assertThat(complete.direct().matchesExpected()).isNull();
        assertThat(complete.review().matchesExpected()).isNull();
        verify(model, times(3)).generate(anyString(), anyString());
    }

    @Test
    void experimentCreationRollsBackBothWorkflowsWhenTheSecondArmFails() {
        jdbc.execute("ALTER TABLE workflows ADD CONSTRAINT fixture_mode CHECK (mode='DIRECT')");
        assertThatThrownBy(() -> experiments.create("request", "draft", null, null)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        for (String table : List.of("workflow_experiments", "workflows", "workflow_steps", "tasks", "outbox")) {
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class)).isZero();
        }
    }

    @Test
    void experimentApiValidatesInputsAndScoresOnlyExactOutputWithSurroundingWhitespaceRemoved() throws Exception {
        var mvc = MockMvcBuilders.standaloneSetup(new ExperimentController(experiments)).addFilters(new ApiKeyFilter("access")).build();
        mvc.perform(post("/api/experiments").servletPath("/api/experiments").contentType("application/json").content("{}"))
                .andExpect(status().isUnauthorized());
        for (String body : List.of("{}", "{\"prompt\":\"hello\"}", "{\"prompt\":\"hello\",\"initialDraft\":\"draft\",\"expectedOutput\":\" \"}",
                "{\"prompt\":\"hello\",\"initialDraft\":\"draft\",\"targetNode\":\"unknown\"}")) {
            mvc.perform(post("/api/experiments").header("X-API-Key", "access").contentType("application/json").content(body)).andExpect(status().isBadRequest());
        }
        assertThat(experiments.list(10)).isEmpty();
        mvc.perform(post("/api/experiments").header("X-API-Key", "access").contentType("application/json")
                .content("{\"prompt\":\"request\",\"initialDraft\":\" answer \",\"expectedOutput\":\"answer\"}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.draftMatchesExpected").value(true));
        mvc.perform(get("/api/experiments?limit=0").header("X-API-Key", "access")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/experiments/" + UUID.randomUUID()).header("X-API-Key", "access")).andExpect(status().isNotFound());
        var experiment = experiments.list(10).getFirst();
        when(model.generate(anyString(), anyString())).thenReturn(" answer \n", "feedback", "Answer");
        String directId = experiment.direct().workflow().workflowId(), reviewId = experiment.review().workflow().workflowId();
        for (String id : List.of(directId, reviewId)) {
            deliverLast(id);
            while (!workflows.get(id).status().equals("SUCCEEDED")) { infer(last(id).task()); deliverLast(id); }
        }
        var result = experiments.get(experiment.experimentId());
        assertThat(result.direct().matchesExpected()).isTrue();
        assertThat(result.review().matchesExpected()).isFalse();
    }

    @Test
    void migrationKeepsOldWorkflowsOnTheReviewPath() {
        var flow = workflows.create("legacy request", null, "legacy draft");
        jdbc.execute("ALTER TABLE workflows DROP COLUMN mode");
        var migration = new ResourceDatabasePopulator(new ClassPathResource("schema.sql"));
        migration.execute(jdbc.getDataSource());
        migration.execute(jdbc.getDataSource());
        var restored = workflows.get(flow.workflowId());
        assertThat(restored.mode()).isEqualTo("REVIEW");
        assertThat(restored.steps().getFirst().task().output()).isEqualTo("legacy draft");
    }
}
