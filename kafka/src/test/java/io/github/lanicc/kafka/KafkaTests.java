package io.github.lanicc.kafka;

import org.junit.jupiter.api.BeforeEach;

/**
 * Created on 2022/4/14.
 *
 * @author lan
 */
public class KafkaTests {

    protected String bootstrapServers = "127.0.0.1:9092";
    protected String serializer = "org.apache.kafka.common.serialization.StringSerializer";
    protected String deserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    @BeforeEach
    void setUp() {

    }
}
