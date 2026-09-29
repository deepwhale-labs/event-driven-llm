package io.github.deepwhalelabs.eventdrivenllm;

import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@EnabledIfSystemProperty(named = "verify.ollama", matches = "true")
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:real-inference;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa", "spring.datasource.password=",
        "spring.kafka.listener.auto-startup=false", "spring.kafka.admin.auto-create=false",
        "app.jobs-enabled=false", "spring.ai.model.chat=ollama",
        "spring.ai.ollama.base-url=${verify.ollama.url}",
        "spring.ai.ollama.chat.options.model=${verify.ollama.model}",
        "spring.ai.ollama.chat.options.num-predict=32"
})
class RealInferenceTest {
    @Autowired TaskStore tasks;
    @Autowired LlmCommandConsumer worker;
    @Autowired CoralResultConsumer notifier;
    @Autowired ObjectMapper mapper;
    @MockitoBean CoralClient coral;

    @Test
    @Timeout(180)
    void realModelResultIsPersistedAndPassedToDelivery() throws Exception {
        Task task = tasks.create("Reply briefly with the word hello.", null);
        worker.consume(mapper.writeValueAsString(new LlmCommand(task.taskId(), task.prompt(), 1)));
        Task generated = tasks.get(task.taskId());
        assertThat(generated.status()).isEqualTo("DELIVERING");
        assertThat(generated.output()).isNotBlank().doesNotContain("[DEMO");
        when(coral.deliver(any())).thenReturn(new CoralClient.Delivery(task.taskId(), UUID.randomUUID().toString(), generated.nodeId(), generated.output()));
        notifier.consume(mapper.writeValueAsString(new LlmResult(task.taskId(), generated.nodeId(), generated.output(), 1)));
        assertThat(tasks.get(task.taskId()).status()).isEqualTo("SUCCEEDED");
        System.out.println("Verified real model output: " + generated.output());
    }
}
