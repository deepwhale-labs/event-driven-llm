package io.github.deepwhalelabs.eventdrivenllm;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ApiTest {
    private TaskStore tasks;
    private MockMvc mvc;
    private final String id = UUID.randomUUID().toString();

    @BeforeEach
    void setup() {
        tasks = mock(TaskStore.class);
        var routing = new Routing("commands", "results", "one", "one,two");
        mvc = MockMvcBuilders.standaloneSetup(new CommandController(tasks), new TaskController(tasks, routing),
                new ResultController(mock(CoralClient.class), tasks), new RuntimeController("ollama", "test-model", 256))
                .addFilters(new ApiKeyFilter("test-key")).build();
    }

    private Task saved() {
        return new Task(id, "hello", "one", "SUCCEEDED", 1, "one", "persisted answer", "thread", null, null, "secret-claim", 100L, 1, 2,
                null, 1L, 1L, 2L, 2L);
    }

    @Test
    void apiRequiresKeyAndDoesNotReturnInternalClaims() throws Exception {
        when(tasks.get(id)).thenReturn(saved());
        mvc.perform(get("/api/tasks/" + id).servletPath("/api/tasks/" + id)).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/tasks/" + id).servletPath("/api/tasks/" + id).header("X-API-Key", "wrong")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/tasks/" + id).servletPath("/api/tasks/" + id).header("X-API-Key", "test-key"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.output").value("persisted answer"))
                .andExpect(jsonPath("$.claimToken").doesNotExist()).andExpect(jsonPath("$.leaseUntil").doesNotExist());
    }

    @Test
    void rejectsInvalidRequestsAndAcceptsDurablyQueuedTask() throws Exception {
        when(tasks.create("hello", "one")).thenReturn(saved());
        mvc.perform(post("/api/commands").servletPath("/api/commands").header("X-API-Key", "test-key")
                .contentType(MediaType.APPLICATION_JSON).content("{\"prompt\":\" \"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/api/commands").servletPath("/api/commands").header("X-API-Key", "test-key")
                .contentType(MediaType.APPLICATION_JSON).content("{\"prompt\":\"hello\",\"targetNode\":\"one\"}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.taskId").value(id));
        verify(tasks, times(1)).create(anyString(), any());
    }

    @Test
    void resultReadsSavedDataWithoutCoralAndRetryIsAccepted() throws Exception {
        when(tasks.get(id)).thenReturn(saved());
        when(tasks.retry(id)).thenReturn(saved());
        mvc.perform(get("/api/results/" + id).header("X-API-Key", "test-key"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.output").value("persisted answer"));
        mvc.perform(post("/api/tasks/" + id + "/retry").header("X-API-Key", "test-key"))
                .andExpect(status().isAccepted());
        mvc.perform(get("/api/tasks?limit=101").header("X-API-Key", "test-key")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/tasks?status=unknown").header("X-API-Key", "test-key")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/nodes").header("X-API-Key", "test-key")).andExpect(jsonPath("$[1]").value("two"));
    }

    @Test
    void batchValidationRejectsTheWholeRequestBeforeAnyWrites() throws Exception {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        for (String body : java.util.List.of("{}", "{\"prompts\":[]}", "{\"prompts\":[\"valid\",null]}",
                "{\"prompts\":[\"valid\",\" \"]}",
                mapper.writeValueAsString(java.util.Map.of("prompts", java.util.Collections.nCopies(51, "hello"))),
                mapper.writeValueAsString(java.util.Map.of("prompts", java.util.List.of("a".repeat(32001)))),
                mapper.writeValueAsString(java.util.Map.of("prompts", java.util.Collections.nCopies(9, "a".repeat(32000)))))) {
            mvc.perform(post("/api/commands/batch").header("X-API-Key", "test-key")
                    .contentType(MediaType.APPLICATION_JSON).content(body)).andExpect(status().isBadRequest());
        }
        verifyNoInteractions(tasks);
    }

    @Test
    void batchSubmissionQueriesAndRuntimeRespectAuthentication() throws Exception {
        var batch = new TaskStore.Batch(new TaskStore.BatchSummary(id, 1, 1, 1, 0), java.util.List.of(saved()));
        when(tasks.createBatch(java.util.List.of("hello"), "one")).thenReturn(batch);
        when(tasks.batch(id)).thenReturn(batch);
        when(tasks.batches(10)).thenReturn(java.util.List.of(batch.summary()));
        mvc.perform(post("/api/commands/batch").servletPath("/api/commands/batch")
                .contentType(MediaType.APPLICATION_JSON).content("{\"prompts\":[\"hello\"]}"))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/api/commands/batch").header("X-API-Key", "test-key").contentType(MediaType.APPLICATION_JSON)
                .content("{\"prompts\":[\"hello\"],\"targetNode\":\"one\"}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.summary.batchId").value(id));
        mvc.perform(get("/api/batches/" + id).header("X-API-Key", "test-key"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.tasks[0].finishedAt").value(2));
        mvc.perform(get("/api/batches").header("X-API-Key", "test-key"))
                .andExpect(status().isOk()).andExpect(jsonPath("$[0].total").value(1));
        mvc.perform(get("/api/batches?limit=51").header("X-API-Key", "test-key")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/runtime").servletPath("/api/runtime")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/runtime").header("X-API-Key", "test-key"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.mode").value("OLLAMA"))
                .andExpect(jsonPath("$.model").value("test-model")).andExpect(jsonPath("$.maxBatchSize").value(50));
    }
}
