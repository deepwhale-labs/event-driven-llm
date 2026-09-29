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
                new ResultController(mock(CoralClient.class), tasks)).addFilters(new ApiKeyFilter("test-key")).build();
    }

    private Task saved() {
        return new Task(id, "hello", "one", "SUCCEEDED", 1, "one", "persisted answer", "thread", null, null, "secret-claim", 100L, 1, 2);
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
}
