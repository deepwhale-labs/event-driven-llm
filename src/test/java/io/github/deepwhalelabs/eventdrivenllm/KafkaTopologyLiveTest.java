package io.github.deepwhalelabs.eventdrivenllm;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in: creates only uniquely named, empty test resources and cleans them in finally. */
@EnabledIfEnvironmentVariable(named = "VERIFY_KAFKA", matches = "true")
class KafkaTopologyLiveTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final String bootstrap = System.getenv().getOrDefault("KAFKA_TEST_BOOTSTRAP", "localhost:9092");
    private final String baseUrl = System.getenv().getOrDefault("KAFKA_TEST_BASE_URL", "http://localhost:18080");

    @Test
    void discoversRealTopicAndConsumersThenRebalanceAndRemoval() throws Exception {
        String name = "topology-verify-" + UUID.randomUUID();
        String group = name + "-group";
        TestConsumer first = null, second = null;
        boolean topicCreated = false;
        try (var admin = Admin.create(Map.of("bootstrap.servers", bootstrap,
                "request.timeout.ms", 5000, "default.api.timeout.ms", 10000))) {
            try {
                admin.createTopics(List.of(new NewTopic(name, 2, (short) 1))).all().get(15, TimeUnit.SECONDS);
                topicCreated = true;
                var added = await(view -> find(view, "topics", "name", name) != null);
                assertThat(find(added, "topics", "name", name).path("partitions").size()).isEqualTo(2);
                first = new TestConsumer(bootstrap, group, name);
                await(view -> memberCount(view, group) == 1);
                second = new TestConsumer(bootstrap, group, name);
                var two = await(view -> memberCount(view, group) == 2
                        && find(view, "groups", "id", group).path("members").get(0).path("assignments").size() == 1
                        && find(view, "groups", "id", group).path("members").get(1).path("assignments").size() == 1);
                var members = find(two, "groups", "id", group).path("members");
                assertThat(members.get(0).path("clientId").asText()).isEqualTo(members.get(1).path("clientId").asText());
                assertThat(members.get(0).path("memberId").asText()).isNotEqualTo(members.get(1).path("memberId").asText());
                java.nio.file.Files.writeString(java.nio.file.Path.of("build/kafka-live-two-consumers.json"), mapper.writeValueAsString(two));
                first.close();
                first = null;
                var rebalanced = await(view -> memberCount(view, group) == 1
                        && find(view, "groups", "id", group).path("members").get(0).path("assignments").size() == 2);
                assertThat(find(rebalanced, "groups", "id", group).path("lag").asLong(-1)).isZero();
                second.close();
                second = null;
                await(view -> memberCount(view, group) == 0);
                admin.deleteConsumerGroups(List.of(group)).all().get(15, TimeUnit.SECONDS);
                admin.deleteTopics(List.of(name)).all().get(15, TimeUnit.SECONDS);
                topicCreated = false;
                await(view -> find(view, "topics", "name", name) == null && find(view, "groups", "id", group) == null);
            } finally {
                // Release both clients even if an assertion or one close fails.
                try { if (first != null) first.close(); }
                finally {
                    try { if (second != null) second.close(); }
                    finally {
                        if (topicCreated) {
                            try { admin.deleteConsumerGroups(List.of(group)).all().get(15, TimeUnit.SECONDS); }
                            finally { admin.deleteTopics(List.of(name)).all().get(15, TimeUnit.SECONDS); }
                        }
                    }
                }
            }
        }
    }

    private JsonNode await(Predicate<JsonNode> condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(55);
        do {
            var builder = HttpRequest.newBuilder(URI.create(baseUrl + "/api/kafka/topology")).timeout(Duration.ofSeconds(10));
            var key = System.getenv("APP_API_KEY");
            if (key != null && !key.isBlank()) builder.header("X-API-Key", key);
            var response = http.send(builder.GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            var view = mapper.readTree(response.body());
            if (view.path("status").asText().equals("CONNECTED") && condition.test(view)) return view;
            Thread.sleep(600);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Kafka topology change was not observed before timeout");
    }

    private static JsonNode find(JsonNode view, String array, String field, String value) {
        for (var item : view.path(array)) if (item.path(field).asText().equals(value)) return item;
        return null;
    }

    private static int memberCount(JsonNode view, String id) {
        var group = find(view, "groups", "id", id);
        return group == null || group.path("memberCount").isNull() ? -1 : group.path("memberCount").asInt(-1);
    }

    private static class TestConsumer implements AutoCloseable {
        private final KafkaConsumer<String, String> consumer;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Thread thread;

        TestConsumer(String bootstrap, String group, String topic) {
            consumer = new KafkaConsumer<>(Map.of("bootstrap.servers", bootstrap, "group.id", group,
                    "client.id", "topology-verification", "enable.auto.commit", "false", "auto.offset.reset", "earliest",
                    "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                    "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"));
            thread = new Thread(() -> {
                try {
                    consumer.subscribe(List.of(topic));
                    while (running.get()) consumer.poll(Duration.ofMillis(250));
                } catch (WakeupException ex) {
                    if (running.get()) failure.set(ex);
                } catch (Throwable ex) {
                    failure.set(ex);
                } finally {
                    consumer.close(Duration.ofSeconds(5));
                }
            }, "topology-test-consumer");
            thread.setDaemon(true);
            thread.start();
        }

        @Override
        public void close() throws InterruptedException {
            running.set(false);
            consumer.wakeup();
            thread.join(10000);
            assertThat(thread.isAlive()).isFalse();
            assertThat(failure.get()).isNull();
        }
    }
}
