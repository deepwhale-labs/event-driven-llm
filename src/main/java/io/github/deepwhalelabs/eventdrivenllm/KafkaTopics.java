package io.github.deepwhalelabs.eventdrivenllm;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaTopics {

    @Bean
    NewTopic commandTopic(@Value("${app.kafka.command-topic}") String name) {
        return TopicBuilder.name(name).partitions(2).replicas(1).build();
    }

    @Bean
    NewTopic resultTopic(@Value("${app.kafka.result-topic}") String name) {
        return TopicBuilder.name(name).partitions(2).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic(@Value("${app.kafka.command-topic}") String name) {
        return TopicBuilder.name(name + ".DLT").partitions(2).replicas(1).build();
    }

    @Bean
    NewTopic resultDeadLetterTopic(@Value("${app.kafka.result-topic}") String name) {
        return TopicBuilder.name(name + ".DLT").partitions(2).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }
}
