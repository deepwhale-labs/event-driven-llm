package io.github.deepwhalelabs.eventdrivenllm;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class CoralWorkflowClient {
    private static final Pattern JSON_BLOCK = Pattern.compile("(?m)^\\s*```json\\s*\\R([\\s\\S]*?)^\\s*```\\s*$");
    private final ObjectMapper mapper;
    private final CoralClient session;

    public CoralWorkflowClient(ObjectMapper mapper, @Value("${app.coral.base-url}") URI base,
            @Value("${app.coral.runtime-dir}") Path runtime) {
        this.mapper = mapper;
        this.session = new CoralClient(mapper, base, runtime, "event-driven-llm-workflows", List.of("writer", "reviewer"));
    }

    // Called under the shared DB Coral lock. Republish durable messages if Coral restarted.
    public synchronized Conversation exchange(String workflowId, List<Message> history, String reader) {
        try {
            String name = "workflow:" + workflowId;
            JsonNode thread = null;
            for (JsonNode item : session.state().path("threads")) {
                if (name.equals(item.path("name").asText())) { thread = item; break; }
            }
            String threadId = thread == null ? session.toolAs("writer", "coral_create_thread", Map.of(
                    "threadName", name, "participantNames", List.of("reviewer"))).path("thread").path("id").asText()
                    : thread.path("id").asText();
            if (threadId.isBlank()) throw new CoralException("Coral workflow thread missing");
            for (Message expected : history) {
                String actor = actor(expected.stage());
                boolean found = false;
                if (thread != null) for (JsonNode item : thread.path("messages")) {
                    if (!actor.equals(item.path("senderName").asText())) continue;
                    Message actual = decode(item.path("text").asText());
                    if (actual != null && expected.taskId().equals(actual.taskId())) {
                        if (!actual.equals(expected)) throw new CoralException("Coral workflow message conflict");
                        found = true;
                    }
                }
                if (!found) session.toolAs(actor, "coral_send_message", Map.of("threadId", threadId,
                        "content", mapper.writeValueAsString(expected),
                        "mentions", List.of(actor.equals("writer") ? "reviewer" : "writer")));
            }
            // The next role reads its own MCP resource, not the application's DB answer.
            var messages = new ArrayList<Message>();
            var blocks = JSON_BLOCK.matcher(session.readAs(reader));
            while (blocks.find()) {
                JsonNode data = mapper.readTree(blocks.group(1));
                if (!data.isArray()) continue;
                for (JsonNode item : data) {
                    if (!name.equals(item.path("threadName").asText()) || !threadId.equals(item.path("threadId").asText())) continue;
                    for (JsonNode entry : item.path("messages")) {
                        Message value = decode(entry.path("messageText").asText());
                        if (value != null && workflowId.equals(value.workflowId())
                                && actor(value.stage()).equals(entry.path("sendingAgentName").asText())) messages.add(value);
                    }
                }
            }
            if (!messages.equals(history)) throw new CoralException("Coral workflow context incomplete or changed");
            return new Conversation(threadId, reader, List.copyOf(messages));
        } catch (java.io.IOException ex) {
            throw new CoralException("Invalid Coral workflow context");
        }
    }

    private Message decode(String text) {
        try { return mapper.readValue(text, Message.class); }
        catch (java.io.IOException ex) { return null; }
    }

    static String actor(String stage) { return "REVIEW".equals(stage) ? "reviewer" : "writer"; }
    public record Message(String workflowId, String taskId, String stage, String request, String content) { }
    public record Conversation(String threadId, String reader, List<Message> messages) { }
}
