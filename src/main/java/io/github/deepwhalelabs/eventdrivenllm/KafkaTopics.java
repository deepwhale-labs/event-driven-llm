package io.github.deepwhalelabs.eventdrivenllm;

import java.util.ArrayList;
import java.util.UUID;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaTopics {
    public static final int RETRY_COUNT = 2;
    @Bean
    KafkaAdmin.NewTopics topics(Routing routing) {
        var names = new ArrayList<String>();
        names.add(routing.commandTopic(null));
        names.add(routing.resultTopic());
        routing.nodes().forEach(node -> names.add(routing.commandTopic(node)));
        var topics = new ArrayList<NewTopic>();
        for (String name : names) {
            topics.add(TopicBuilder.name(name).partitions(2).replicas(1).build());
            topics.add(TopicBuilder.name(name + ".DLT").partitions(2).replicas(1).build());
        }
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafka, TaskStore tasks,
            ObjectMapper mapper, Routing routing) {
        var dlt = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        dlt.setFailIfSendResultIsError(true);
        return new DefaultErrorHandler((record, exception) -> {
            dlt.accept(record, exception);
            String id;
            int attempt;
            try {
                var event = mapper.readTree(String.valueOf(record.value()));
                if (event == null || !event.isObject()) return;
                id = UUID.fromString(event.path("taskId").asText()).toString();
                attempt = event.path("attempt").asInt(1);
                if (attempt == 0) attempt = 1;
            } catch (JsonProcessingException | IllegalArgumentException ex) {
                return;
            }
            tasks.fail(id, attempt, record.topic().equals(routing.resultTopic()) ? "DELIVERY" : "COMMAND");
        }, new FixedBackOff(1000L, RETRY_COUNT));
    }
}
