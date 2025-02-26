package com.dpadd;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

import static com.dpadd.Topics.INPUT_TOPIC;
import static com.dpadd.Topics.OUTPUT_TOPIC;

@Slf4j
@Configuration
public class KafkaStreamsConfig {

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> log.info("Received message: k={} v={}", k, v))

                .flatMapValues(value -> Arrays.asList(value.split(" ")))

                .peek((k, v) -> log.info("About to send a message: k={} v={}", k, v))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}