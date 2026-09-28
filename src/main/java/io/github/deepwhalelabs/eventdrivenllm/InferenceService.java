package io.github.deepwhalelabs.eventdrivenllm;

import org.springframework.ai.chat.model.ChatModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class InferenceService {
    private final ChatModel model;

    public InferenceService(ObjectProvider<ChatModel> models,
            @Value("${spring.ai.model.chat}") String mode) {
        if (!mode.equals("none") && !mode.equals("ollama")) {
            throw new IllegalArgumentException("SPRING_AI_MODEL_CHAT must be none or ollama");
        }
        this.model = mode.equals("none") ? null : models.getObject();
    }

    public String generate(String prompt) {
        return model == null ? "[DEMO - no model inference] Received: " + prompt : model.call(prompt);
    }
}
