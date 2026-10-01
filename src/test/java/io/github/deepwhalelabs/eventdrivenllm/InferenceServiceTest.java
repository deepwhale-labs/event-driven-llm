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
    void roleInstructionsAreSentAsSystemMessagesAndContextAsUserMessages() {
        ChatModel model = mock(ChatModel.class);
        var provider = new StaticListableBeanFactory(java.util.Map.of("model", model)).getBeanProvider(ChatModel.class);
        when(model.call(org.mockito.ArgumentMatchers.any(org.springframework.ai.chat.prompt.Prompt.class))).thenAnswer(call -> {
            org.springframework.ai.chat.prompt.Prompt prompt = call.getArgument(0);
            assertThat(prompt.getInstructions()).hasSize(2);
            assertThat(prompt.getInstructions().get(0).getMessageType()).isEqualTo(org.springframework.ai.chat.messages.MessageType.SYSTEM);
            assertThat(prompt.getInstructions().get(0).getText()).isEqualTo("review this answer");
            assertThat(prompt.getInstructions().get(1).getMessageType()).isEqualTo(org.springframework.ai.chat.messages.MessageType.USER);
            assertThat(prompt.getInstructions().get(1).getText()).isEqualTo("request and draft");
            return new org.springframework.ai.chat.model.ChatResponse(java.util.List.of(new org.springframework.ai.chat.model.Generation(
                    new org.springframework.ai.chat.messages.AssistantMessage("feedback"))));
        });
        assertThat(new InferenceService(provider, config("ollama"), null).generate("review this answer", "request and draft")).isEqualTo("feedback");
    }
    @Test
    void demoWorksWithoutModelAndLabelsItsOutput() {
        var provider = new StaticListableBeanFactory().getBeanProvider(ChatModel.class);
        assertThat(new InferenceService(provider, config("none"), null).generate("hello")).startsWith("[DEMO - no model inference]");
        assertThatThrownBy(() -> new InferenceService(provider, config("ollama"), null)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> config("typo")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ollamaFailureNeverFallsBackToDemo() {
        ChatModel model = mock(ChatModel.class);
        when(model.call("hello")).thenThrow(new IllegalStateException("offline"));
        var provider = new StaticListableBeanFactory(java.util.Map.of("model", model)).getBeanProvider(ChatModel.class);
        assertThatThrownBy(() -> new InferenceService(provider, config("ollama"), null).generate("hello"))
                .isInstanceOf(IllegalStateException.class).hasMessage("offline");
    }

    @Test
    void codexUsesCliWithoutOllamaAndNeverFallsBackOnFailure() {
        var cli = mock(CodexCliClient.class);
        var service = new InferenceService(new StaticListableBeanFactory().getBeanProvider(ChatModel.class), config("codex-cli"), cli);
        when(cli.generate("review", "draft")).thenReturn("correct it");
        when(cli.generate(null, "hello")).thenThrow(new IllegalStateException("CLI offline"));
        assertThat(service.generate("review", "draft")).isEqualTo("correct it");
        assertThatThrownBy(() -> service.generate("hello")).hasMessage("CLI offline");
        var runtime = new RuntimeController(config("codex-cli")).runtime();
        assertThat(runtime.mode()).isEqualTo("CODEX_CLI");
        assertThat(runtime.model()).isEqualTo("Codex CLI / fixture-cli");
        assertThat(runtime.maxOutputTokens()).isNull();
    }

    private static InferenceConfig config(String provider) { return new InferenceConfig(provider, "test-model", 256, "fixture-cli"); }
}
