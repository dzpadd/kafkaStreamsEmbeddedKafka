package com.dpadd;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static com.dpadd.Topics.INPUT_TOPIC;
import static com.dpadd.Topics.OUTPUT_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@SpringBootTest(properties = {
        // connect to embedded kafka instead of a real one
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",

        // suppress most of embedded kafka network connection logs
        "logging.level.kafka=WARN",
        "logging.level.state.change.logger=WARN"
})
@EmbeddedKafka(kraft = true, partitions = 1, topics = {"input-topic", OUTPUT_TOPIC})
class AppTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Test
    void shouldProcessValidMessage() {
        // given
        var key = RandomStringUtils.secure().nextAlphabetic(3);
        var value = "Hello Kafka Streams";

        var consumer = consumerFactory.createConsumer("test", "appId");
        consumer.subscribe(List.of(OUTPUT_TOPIC));

        // when
        kafkaTemplate.send(INPUT_TOPIC, key, value);

        // then
        var records = KafkaTestUtils.getRecords(consumer, Duration.of(2, ChronoUnit.SECONDS));
        assertThat(records.count()).isEqualTo(3);
        assertThat(records)
                .extracting(ConsumerRecord::key, ConsumerRecord::value)
                .containsExactly(
                        tuple(key, "Hello"),
                        tuple(key, "Kafka"),
                        tuple(key, "Streams"));
    }
}