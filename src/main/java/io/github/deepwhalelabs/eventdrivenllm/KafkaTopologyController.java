package io.github.deepwhalelabs.eventdrivenllm;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Metadata only: never consumes messages, creates topics or changes offsets. */
@RestController
public class KafkaTopologyController {
    private static final int REQUEST_TIMEOUT_MS = 1800;
    private static final long CACHE_MS = 5000;
    private static final Comparator<TopicPartition> PARTITION_ORDER = Comparator.comparing(TopicPartition::topic)
            .thenComparingInt(TopicPartition::partition);
    private final Admin admin;
    private final Clock clock;
    private final Map<String, TopicSpec> configured = new LinkedHashMap<>();
    private final List<String> configuredGroups;
    private final List<Route> routes;
    private Snapshot cached;
    private long cacheUntil;

    @Autowired
    public KafkaTopologyController(KafkaAdmin kafka, Routing routing,
            @Value("${spring.kafka.consumer.group-id}") String workerGroup) {
        this(newAdmin(kafka), routing, workerGroup);
    }

    KafkaTopologyController(Admin admin, Routing routing, String workerGroup) {
        this(admin, routing, workerGroup, Clock.systemUTC());
    }

    KafkaTopologyController(Admin admin, Routing routing, String workerGroup, Clock clock) {
        this.admin = admin;
        this.clock = clock;
        configuredGroups = List.of(workerGroup, CoralResultConsumer.GROUP_ID);
        configured.put(routing.commandTopic(null), new TopicSpec("COMMAND", null, workerGroup));
        routing.nodes().forEach(node -> configured.put(routing.commandTopic(node), new TopicSpec("COMMAND", node, workerGroup)));
        configured.put(routing.resultTopic(), new TopicSpec("RESULT", null, CoralResultConsumer.GROUP_ID));
        var paths = new ArrayList<Route>();
        for (var entry : List.copyOf(configured.entrySet())) {
            String topic = entry.getKey();
            TopicSpec spec = entry.getValue();
            // Both commands and results are published by the durable DB outbox.
            paths.add(new Route("OUTBOX", null, topic, "PUBLISH"));
            paths.add(new Route("GROUP", spec.groupId(), topic + ".DLT", "FAILURE"));
            configured.put(topic + ".DLT", new TopicSpec("DLT", spec.targetNode(), null));
        }
        routes = List.copyOf(paths);
    }

    private static Admin newAdmin(KafkaAdmin kafka) {
        var config = new HashMap<>(kafka.getConfigurationProperties());
        config.put("client.id", "kafka-topology-view");
        config.put("request.timeout.ms", REQUEST_TIMEOUT_MS);
        config.put("default.api.timeout.ms", REQUEST_TIMEOUT_MS);
        return Admin.create(config);
    }

    @PreDestroy
    public void close() { admin.close(Duration.ofSeconds(1)); }

    @GetMapping("/api/kafka/topology")
    public synchronized Snapshot topology() {
        if (cached != null && clock.millis() < cacheUntil) return cached;
        cached = inspect();
        cacheUntil = clock.millis() + CACHE_MS;
        return cached;
    }

    private Snapshot inspect() {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(6500);
        try {
            var cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs(REQUEST_TIMEOUT_MS));
            var listedTopics = admin.listTopics(new ListTopicsOptions().listInternal(false).timeoutMs(REQUEST_TIMEOUT_MS)).names();
            var listedGroups = admin.listConsumerGroups(new ListConsumerGroupsOptions().timeoutMs(REQUEST_TIMEOUT_MS)).all();
            var nodes = await(cluster.nodes(), deadline);
            if (nodes == null) return unavailable();
            var topicNames = await(listedTopics, deadline);
            var groupListings = await(listedGroups, deadline);
            boolean discoveryComplete = topicNames != null && groupListings != null;
            // A failed list request must not be interpreted as deletion of every node.
            var names = new TreeSet<String>();
            if (topicNames != null) names.addAll(topicNames);
            else {
                names.addAll(configured.keySet());
                if (cached != null) cached.topics().forEach(t -> names.add(t.name()));
            }
            var ids = new TreeSet<String>();
            if (groupListings != null) groupListings.forEach(g -> ids.add(g.groupId()));
            else {
                ids.addAll(configuredGroups);
                if (cached != null) cached.groups().forEach(g -> ids.add(g.id()));
            }
            var topics = names.isEmpty() ? Map.<String, KafkaFuture<TopicDescription>>of() : admin.describeTopics(names,
                    new DescribeTopicsOptions().timeoutMs(REQUEST_TIMEOUT_MS)).topicNameValues();
            var descriptions = ids.isEmpty() ? Map.<String, KafkaFuture<ConsumerGroupDescription>>of() : admin.describeConsumerGroups(ids,
                    new DescribeConsumerGroupsOptions().timeoutMs(REQUEST_TIMEOUT_MS)).describedGroups();
            var offsets = ids.isEmpty() ? null : admin.listConsumerGroupOffsets(ids.stream().collect(Collectors.toMap(Function.identity(),
                    id -> new ListConsumerGroupOffsetsSpec())), new ListConsumerGroupOffsetsOptions().timeoutMs(REQUEST_TIMEOUT_MS));
            Node controller = await(cluster.controller(), deadline);
            String clusterId = await(cluster.clusterId(), deadline);
            var brokers = nodes.stream().sorted(Comparator.comparingInt(Node::id))
                    .map(n -> new Broker(n.id(), n.host(), n.port(), controller != null && n.id() == controller.id())).toList();
            var described = new LinkedHashMap<String, TopicDescription>();
            var partitionRequests = new HashMap<TopicPartition, OffsetSpec>();
            for (String name : names) {
                var description = await(topics.get(name), deadline);
                described.put(name, description);
                if (description != null) description.partitions().forEach(p ->
                        partitionRequests.put(new TopicPartition(name, p.partition()), OffsetSpec.latest()));
            }
            var ends = partitionRequests.isEmpty() ? null : admin.listOffsets(partitionRequests,
                    new ListOffsetsOptions().timeoutMs(REQUEST_TIMEOUT_MS));
            var endOffsets = new HashMap<TopicPartition, Long>();
            for (var key : partitionRequests.keySet()) {
                var info = await(ends.partitionResult(key), deadline);
                endOffsets.put(key, info == null ? null : info.offset());
            }
            var committed = new HashMap<String, Map<TopicPartition, OffsetAndMetadata>>();
            var groups = new HashMap<String, ConsumerGroupDescription>();
            for (String id : ids) {
                committed.put(id, await(offsets.partitionsToOffsetAndMetadata(id), deadline));
                groups.put(id, await(descriptions.get(id), deadline));
            }
            var topicViews = new ArrayList<Topic>();
            for (String name : names) {
                var spec = configured.getOrDefault(name, new TopicSpec("OTHER", null, null));
                var description = described.get(name);
                if (description == null) {
                    topicViews.add(new Topic(name, spec.kind(), spec.targetNode(), spec.groupId(), "UNKNOWN", List.of()));
                    continue;
                }
                var partitions = description.partitions().stream().sorted(Comparator.comparingInt(p -> p.partition())).map(p -> {
                    var key = new TopicPartition(name, p.partition());
                    var groupOffsets = committed.get(spec.groupId());
                    Long commit = commitOffset(groupOffsets, key);
                    Long end = endOffsets.get(key);
                    return new Partition(p.partition(), p.leader() == null || p.leader().id() < 0 ? null : p.leader().id(),
                            p.replicas().stream().map(Node::id).toList(), p.isr().stream().map(Node::id).toList(),
                            end, commit, groupOffsets == null ? null : lag(end, commit));
                }).toList();
                topicViews.add(new Topic(name, spec.kind(), spec.targetNode(), spec.groupId(), "AVAILABLE", partitions));
            }
            var groupViews = new ArrayList<Group>();
            for (String id : ids) {
                var description = groups.get(id);
                var members = description == null ? List.<Member>of() : description.members().stream()
                        .sorted(Comparator.comparing(MemberDescription::consumerId))
                        .map(m -> new Member(m.consumerId(), m.clientId(), m.assignment().topicPartitions().stream()
                                .filter(p -> names.contains(p.topic())).sorted(PARTITION_ORDER)
                                .map(p -> new Assignment(p.topic(), p.partition())).toList())).toList();
                var keys = new TreeSet<TopicPartition>(PARTITION_ORDER);
                var groupOffsets = committed.get(id);
                if (groupOffsets != null) keys.addAll(groupOffsets.keySet());
                members.forEach(m -> m.assignments().forEach(a -> keys.add(new TopicPartition(a.topic(), a.partition()))));
                // Include uncommitted partitions of every known consumed topic in its lag.
                var consumedTopics = keys.stream().map(TopicPartition::topic).collect(Collectors.toSet());
                configured.forEach((name, spec) -> { if (id.equals(spec.groupId())) consumedTopics.add(name); });
                partitionRequests.keySet().stream().filter(p -> consumedTopics.contains(p.topic())).forEach(keys::add);
                keys.removeIf(p -> !names.contains(p.topic())); // Ignore commits retained for deleted/internal topics.
                var progress = keys.stream().map(key -> {
                    Long commit = commitOffset(groupOffsets, key);
                    return new Progress(key.topic(), key.partition(), commit,
                            groupOffsets == null ? null : lag(endOffsets.get(key), commit));
                }).toList();
                boolean complete = description != null && groupOffsets != null && discoveryComplete
                        && consumedTopics.stream().filter(names::contains).allMatch(n -> described.get(n) != null)
                        && progress.stream().allMatch(p -> p.lag() != null);
                Long lag = complete ? progress.stream().mapToLong(Progress::lag).sum() : null;
                groupViews.add(new Group(id, description == null ? "UNKNOWN" : description.state().toString(),
                        description == null ? null : members.size(), lag, members, progress));
            }
            boolean partial = !discoveryComplete || controller == null || clusterId == null
                    || topicViews.stream().anyMatch(t -> t.state().equals("UNKNOWN"))
                    || groupViews.stream().anyMatch(g -> g.state().equals("UNKNOWN"))
                    || committed.values().stream().anyMatch(v -> v == null) || endOffsets.values().stream().anyMatch(v -> v == null);
            return new Snapshot(partial ? "PARTIAL" : "CONNECTED", clock.millis(), clusterId, brokers,
                    topicViews, groupViews, KafkaTopics.RETRY_COUNT, discoveryComplete, routes);
        } catch (RuntimeException ex) {
            // Never expose broker exception bodies or authentication configuration.
            return unavailable();
        }
    }

    private static Long commitOffset(Map<TopicPartition, OffsetAndMetadata> offsets, TopicPartition key) {
        var commit = offsets == null ? null : offsets.get(key);
        return commit == null || commit.offset() < 0 ? null : commit.offset();
    }

    static Long lag(Long endOffset, Long committedOffset) {
        if (endOffset == null) return null;
        if (committedOffset == null) return endOffset == 0 ? 0L : null;
        return Math.max(0, endOffset - committedOffset);
    }

    private Snapshot unavailable() {
        // The browser keeps its last successful graph, clearly labelled as stale.
        return new Snapshot("UNAVAILABLE", clock.millis(), null, List.of(), List.of(), List.of(),
                KafkaTopics.RETRY_COUNT, false, routes);
    }

    private static <T> T await(KafkaFuture<T> future, long deadline) {
        if (future == null) return null;
        try {
            return future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception ex) {
            return null;
        }
    }

    private record TopicSpec(String kind, String targetNode, String groupId) { }
    public record Snapshot(String status, long checkedAt, String clusterId, List<Broker> brokers,
            List<Topic> topics, List<Group> groups, int retries, boolean discoveryComplete, List<Route> routes) { }
    public record Broker(int id, String host, int port, boolean controller) { }
    public record Topic(String name, String kind, String targetNode, String groupId, String state, List<Partition> partitions) { }
    public record Partition(int id, Integer leader, List<Integer> replicas, List<Integer> inSyncReplicas,
            Long endOffset, Long committedOffset, Long lag) { }
    public record Group(String id, String state, Integer memberCount, Long lag, List<Member> members, List<Progress> offsets) { }
    public record Member(String memberId, String clientId, List<Assignment> assignments) { }
    public record Assignment(String topic, int partition) { }
    public record Progress(String topic, int partition, Long committedOffset, Long lag) { }
    public record Route(String sourceKind, String sourceId, String targetTopic, String kind) { }
}
