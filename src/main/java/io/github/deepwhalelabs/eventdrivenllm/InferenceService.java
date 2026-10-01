package io.github.deepwhalelabs.eventdrivenllm;

import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.prompt.Prompt;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

@Service
public class InferenceService {
    private final ChatModel model;
    private final CodexCliClient cli;
    private final InferenceConfig config;

    public InferenceService(ObjectProvider<ChatModel> models,
            InferenceConfig config, CodexCliClient cli) {
        this.config = config;
        this.cli = cli;
        this.model = config.provider().equals("ollama") ? models.getObject() : null;
    }

    public String generate(String prompt) {
        if (config.provider().equals("codex-cli")) return cli.generate(null, prompt);
        return model == null ? "[DEMO - no model inference] Received: " + prompt : model.call(prompt);
    }

    public String generate(String system, String prompt) {
        if (config.provider().equals("codex-cli")) return cli.generate(system, prompt);
        if (model == null) return generate(prompt);
        return model.call(new Prompt(List.of(new SystemMessage(system), new UserMessage(prompt)))).getResult().getOutput().getText();
    }
}
