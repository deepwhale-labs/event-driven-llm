package io.github.deepwhalelabs.eventdrivenllm;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

final class CoralMcp {
    private final CoralHttp http;
    private final URI endpoint;
    private final Map<String, String> headers = new HashMap<>();
    private long sequence;

    CoralMcp(CoralHttp http, URI endpoint) {
        this.http = http;
        this.endpoint = endpoint;
        JsonNode initialized = call("initialize", Map.of("protocolVersion", "2025-06-18",
                "capabilities", Map.of(), "clientInfo", Map.of("name", "event-driven-llm", "version", "0.1.0")));
        headers.put("MCP-Protocol-Version", initialized.path("protocolVersion").asText("2025-06-18"));
        http.request(endpoint, headers,
                Map.of("jsonrpc", "2.0", "method", "notifications/initialized", "params", Map.of()), false);
    }

    JsonNode tool(String name, Map<String, Object> arguments) {
        JsonNode result = call("tools/call", Map.of("name", name, "arguments", arguments));
        if (result.path("isError").asBoolean()) {
            throw new CoralException("Coral MCP tool failed");
        }
        return result.path("structuredContent");
    }

    private JsonNode call(String method, Object params) {
        long id = ++sequence;
        var reply = http.request(endpoint, headers,
                Map.of("jsonrpc", "2.0", "id", id, "method", method, "params", params), false);
        if (reply.sessionId() != null) {
            headers.put("Mcp-Session-Id", reply.sessionId());
        }
        JsonNode body = reply.body();
        if (body.has("error") || body.path("id").asLong(-1) != id || !body.has("result")) {
            throw new CoralException("Coral MCP request failed");
        }
        return body.get("result");
    }
}
