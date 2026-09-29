package io.github.deepwhalelabs.eventdrivenllm;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class KafkaTopologyControllerTest {
    private Admin admin;
    private KafkaTopologyController controller;
    private Map<String, KafkaFuture<TopicDescription>> topics;
    private Map<String, Map<TopicPartition, OffsetAndMetadata>> committed;
    private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets;
    private Map<String, KafkaFuture<ConsumerGroupDescription>> groups;
    private java.time.Clock clock;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        admin = mock(Admin.class);
        clock = mock(java.time.Clock.class);
        when(clock.millis()).thenReturn(10000L);
        controller = new KafkaTopologyController(admin, new Routing("commands", "results", "one", "one"), "workers", clock);
        Node broker = new Node(1, "broker", 19092);
        var cluster = mock(DescribeClusterResult.class);
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenReturn(cluster);
        when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(broker)));
        when(cluster.controller()).thenReturn(KafkaFuture.completedFuture(broker));
        when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture("test-cluster"));

        topics = new HashMap<>();
        endOffsets = new HashMap<>();
        committed = new HashMap<>();
        committed.put("workers", new HashMap<>());
        committed.put("coral-notifier", new HashMap<>());
        for (String name : List.of("commands", "commands.one", "results", "commands.DLT", "commands.one.DLT", "results.DLT")) {
            topics.put(name, KafkaFuture.completedFuture(new TopicDescription(name, false,
                    List.of(new TopicPartitionInfo(0, broker, List.of(broker), List.of(broker)),
                            new TopicPartitionInfo(1, broker, List.of(broker), List.of(broker))))));
            for (int p = 0; p < 2; p++) {
                var key = new TopicPartition(name, p);
                endOffsets.put(key, new ListOffsetsResult.ListOffsetsResultInfo(10, -1, java.util.Optional.empty()));
                if (!name.endsWith(".DLT")) committed.get(name.equals("results") ? "coral-notifier" : "workers").put(key, new OffsetAndMetadata(7));
            }
        }
        var topicResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(any(Collection.class), any(DescribeTopicsOptions.class))).thenReturn(topicResult);
        when(topicResult.topicNameValues()).thenReturn(topics);
        var topicListing = mock(ListTopicsResult.class);
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(topicListing);
        when(topicListing.names()).thenAnswer(invocation -> KafkaFuture.completedFuture(Set.copyOf(topics.keySet())));
        var endResult = mock(ListOffsetsResult.class);
        when(admin.listOffsets(anyMap(), any(ListOffsetsOptions.class))).thenReturn(endResult);
        when(endResult.partitionResult(any())).thenAnswer(invocation -> KafkaFuture.completedFuture(endOffsets.get(invocation.getArgument(0))));

        var groupResult = mock(DescribeConsumerGroupsResult.class);
        groups = new HashMap<>();
        for (String id : List.of("workers", "coral-notifier")) {
            var member = mock(MemberDescription.class);
            when(member.consumerId()).thenReturn(id + "-consumer");
            when(member.clientId()).thenReturn(id + "-client");
            when(member.assignment()).thenReturn(new MemberAssignment(Set.of(new TopicPartition(id.equals("workers") ? "commands" : "results", 0))));
            var description = mock(ConsumerGroupDescription.class);
            when(description.state()).thenReturn(ConsumerGroupState.STABLE);
            when(description.members()).thenReturn(List.of(member));
            groups.put(id, KafkaFuture.completedFuture(description));
        }
        when(admin.describeConsumerGroups(anyCollection(), any(DescribeConsumerGroupsOptions.class))).thenReturn(groupResult);
        when(groupResult.describedGroups()).thenReturn(groups);
        var groupListing = mock(ListConsumerGroupsResult.class);
        when(admin.listConsumerGroups(any(ListConsumerGroupsOptions.class))).thenReturn(groupListing);
        when(groupListing.all()).thenAnswer(invocation -> KafkaFuture.completedFuture(groups.keySet().stream()
                .map(id -> new ConsumerGroupListing(id, false)).toList()));
        var offsetResult = mock(ListConsumerGroupOffsetsResult.class);
        when(admin.listConsumerGroupOffsets(anyMap(), any(ListConsumerGroupOffsetsOptions.class))).thenReturn(offsetResult);
        when(offsetResult.partitionsToOffsetAndMetadata(anyString())).thenAnswer(invocation -> KafkaFuture.completedFuture(committed.get(invocation.getArgument(0))));
    }

    @Test
    void reportsLiveBrokersTopicsAssignmentsAndLagAndCachesReads() {
        var view = controller.topology();
        assertThat(view.status()).isEqualTo("CONNECTED");
        assertThat(view.brokers()).containsExactly(new KafkaTopologyController.Broker(1, "broker", 19092, true));
        assertThat(view.topics()).hasSize(6);
        assertThat(view.groups().stream().filter(g -> g.id().equals("workers")).findFirst().orElseThrow().lag()).isEqualTo(12);
        assertThat(view.groups().stream().filter(g -> g.id().equals("coral-notifier")).findFirst().orElseThrow().lag()).isEqualTo(6);
        assertThat(view.groups().stream().filter(g -> g.id().equals("workers")).findFirst().orElseThrow().members().getFirst().assignments())
                .containsExactly(new KafkaTopologyController.Assignment("commands", 0));
        assertThat(view.topics().stream().filter(t -> t.kind().equals("DLT")).flatMap(t -> t.partitions().stream())).allMatch(p -> p.lag() == null);
        assertThat(controller.topology()).isSameAs(view);
        verify(admin, times(1)).describeCluster(any(DescribeClusterOptions.class));
    }

    @Test
    void missingCommitIsUnknownRatherThanZeroBacklog() {
        committed.get("workers").remove(new TopicPartition("commands", 0));
        var view = controller.topology();
        assertThat(view.topics().getFirst().partitions().getFirst().lag()).isNull();
        assertThat(view.groups().stream().filter(g -> g.id().equals("workers")).findFirst().orElseThrow().lag()).isNull();
        assertThat(KafkaTopologyController.lag(null, 5L)).isNull();
        assertThat(KafkaTopologyController.lag(0L, null)).isZero();
        assertThat(KafkaTopologyController.lag(7L, 10L)).isZero();
    }

    @Test
    void missingTopicKeepsOtherMetadataAndMarksSnapshotPartial() {
        topics.put("commands.one", KafkaFuture.completedFuture(null));
        var view = controller.topology();
        assertThat(view.status()).isEqualTo("PARTIAL");
        assertThat(view.topics().stream().filter(t -> t.name().equals("commands.one")).findFirst().orElseThrow().state()).isEqualTo("UNKNOWN");
        assertThat(view.groups().stream().filter(g -> g.id().equals("workers")).findFirst().orElseThrow().lag()).isNull();
        assertThat(view.brokers()).hasSize(1);
    }

    @Test
    void failedConnectionReturnsConfiguredRoutesWithoutInventingMetricsOrLeakingDetails() throws Exception {
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenThrow(new IllegalStateException("secret-password broker-body"));
        var view = controller.topology();
        assertThat(view.status()).isEqualTo("UNAVAILABLE");
        assertThat(view.topics()).isEmpty();
        assertThat(view.groups()).isEmpty();
        assertThat(view.discoveryComplete()).isFalse();
        assertThat(view.routes()).hasSize(6);
        assertThat(new ObjectMapper().writeValueAsString(view)).doesNotContain("secret-password", "broker-body");
    }

    @Test
    void topologyUsesTheExistingApiKeyProtection() throws Exception {
        var mvc = MockMvcBuilders.standaloneSetup(controller).addFilters(new ApiKeyFilter("test-key")).build();
        mvc.perform(get("/api/kafka/topology").servletPath("/api/kafka/topology")).andExpect(status().isUnauthorized());
        verifyNoInteractions(admin);
        mvc.perform(get("/api/kafka/topology").servletPath("/api/kafka/topology").header("X-API-Key", "test-key"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.brokers[0].id").value(1))
                .andExpect(jsonPath("$.topics[0].partitions[0].lag").value(3));
    }

    @Test
    void discoversAddedAndRemovedTopicsGroupsAndMemberReassignmentAfterCacheExpires() {
        var initial = controller.topology();
        Node broker = new Node(1, "broker", 19092);
        topics.put("new-events", KafkaFuture.completedFuture(new TopicDescription("new-events", false,
                List.of(new TopicPartitionInfo(0, broker, List.of(broker), List.of(broker)),
                        new TopicPartitionInfo(1, broker, List.of(broker), List.of(broker))))));
        endOffsets.put(new TopicPartition("new-events", 0), new ListOffsetsResult.ListOffsetsResultInfo(9, -1, java.util.Optional.empty()));
        endOffsets.put(new TopicPartition("new-events", 1), new ListOffsetsResult.ListOffsetsResultInfo(9, -1, java.util.Optional.empty()));
        committed.put("new-group", Map.of(new TopicPartition("new-events", 0), new OffsetAndMetadata(7),
                new TopicPartition("new-events", 1), new OffsetAndMetadata(5)));
        setMembers("new-group", List.of(member("first", "shared-client", "new-events", 0), member("second", "shared-client", "new-events", 1)));
        assertThat(controller.topology()).isSameAs(initial);
        when(clock.millis()).thenReturn(16000L);
        var added = controller.topology();
        assertThat(added.topics()).hasSize(7).anyMatch(t -> t.name().equals("new-events") && t.kind().equals("OTHER"));
        var group = added.groups().stream().filter(g -> g.id().equals("new-group")).findFirst().orElseThrow();
        assertThat(group.memberCount()).isEqualTo(2);
        assertThat(group.members()).extracting(KafkaTopologyController.Member::memberId).containsExactly("first", "second");
        assertThat(group.lag()).isEqualTo(6);
        setMembers("new-group", List.of(member("second", "shared-client", "new-events", 0, 1)));
        when(clock.millis()).thenReturn(22000L);
        group = controller.topology().groups().stream().filter(g -> g.id().equals("new-group")).findFirst().orElseThrow();
        assertThat(group.memberCount()).isEqualTo(1);
        assertThat(group.members().getFirst().assignments()).hasSize(2);
        topics.remove("new-events");
        groups.remove("new-group");
        when(clock.millis()).thenReturn(28000L);
        var removed = controller.topology();
        assertThat(removed.topics()).hasSize(6).noneMatch(t -> t.name().equals("new-events"));
        assertThat(removed.groups()).hasSize(2).noneMatch(g -> g.id().equals("new-group"));
    }

    @Test
    void committedHistoryDoesNotInventActiveMembersAndLagIsPerGroup() {
        setMembers("offline", List.of());
        committed.put("offline", Map.of(new TopicPartition("commands", 0), new OffsetAndMetadata(9)));
        var view = controller.topology();
        var group = view.groups().stream().filter(g -> g.id().equals("offline")).findFirst().orElseThrow();
        assertThat(group.members()).isEmpty();
        assertThat(group.offsets()).filteredOn(p -> p.partition() == 0).extracting(KafkaTopologyController.Progress::lag).containsExactly(1L);
        assertThat(group.lag()).isNull(); // P1 has no commit, so the complete backlog is unknown.
    }

    @Test
    void discoveryFailureKeepsKnownNamesAndMarksSnapshotPartial() {
        controller.topology();
        var listing = mock(ListTopicsResult.class);
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(listing);
        when(listing.names()).thenReturn(KafkaFuture.completedFuture(null));
        when(clock.millis()).thenReturn(16000L);
        var view = controller.topology();
        assertThat(view.status()).isEqualTo("PARTIAL");
        assertThat(view.discoveryComplete()).isFalse();
        assertThat(view.topics()).hasSize(6);
        assertThat(view.groups()).allMatch(g -> g.lag() == null);
    }

    @Test
    void emptyClusterIsEmptyAndOneFailedOffsetDoesNotHideOtherPartitions() {
        endOffsets.remove(new TopicPartition("commands", 1));
        var partial = controller.topology();
        assertThat(partial.status()).isEqualTo("PARTIAL");
        assertThat(partial.topics().getFirst().partitions().getFirst().endOffset()).isEqualTo(10);
        topics.clear();
        groups.clear();
        when(clock.millis()).thenReturn(16000L);
        var empty = controller.topology();
        assertThat(empty.status()).isEqualTo("CONNECTED");
        assertThat(empty.topics()).isEmpty();
        assertThat(empty.groups()).isEmpty();
    }

    private MemberDescription member(String id, String client, String topic, int... partitions) {
        var member = mock(MemberDescription.class);
        when(member.consumerId()).thenReturn(id);
        when(member.clientId()).thenReturn(client);
        when(member.assignment()).thenReturn(new MemberAssignment(java.util.Arrays.stream(partitions)
                .mapToObj(p -> new TopicPartition(topic, p)).collect(java.util.stream.Collectors.toSet())));
        return member;
    }

    private void setMembers(String id, List<MemberDescription> members) {
        var group = mock(ConsumerGroupDescription.class);
        when(group.state()).thenReturn(members.isEmpty() ? ConsumerGroupState.EMPTY : ConsumerGroupState.STABLE);
        when(group.members()).thenReturn(members);
        groups.put(id, KafkaFuture.completedFuture(group));
    }
}
