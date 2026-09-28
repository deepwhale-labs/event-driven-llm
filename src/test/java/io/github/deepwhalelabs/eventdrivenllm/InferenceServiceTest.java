package io.github.deepwhalelabs.eventdrivenllm;

import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InferenceServiceTest {
    @Test
    void demoWorksWithoutModelAndLabelsItsOutput() {
        var provider = new StaticListableBeanFactory().getBeanProvider(ChatModel.class);
        assertThat(new InferenceService(provider, "none").generate("hello")).startsWith("[DEMO - no model inference]");
        assertThatThrownBy(() -> new InferenceService(provider, "ollama")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> new InferenceService(provider, "typo")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ollamaFailureNeverFallsBackToDemo() {
        ChatModel model = mock(ChatModel.class);
        when(model.call("hello")).thenThrow(new IllegalStateException("offline"));
        var provider = new StaticListableBeanFactory(java.util.Map.of("model", model)).getBeanProvider(ChatModel.class);
        assertThatThrownBy(() -> new InferenceService(provider, "ollama").generate("hello"))
                .isInstanceOf(IllegalStateException.class).hasMessage("offline");
    }
}
