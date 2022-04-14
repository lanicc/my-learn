package io.github.lanicc.kafka.case1;

import io.github.lanicc.kafka.KafkaTests;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created on 2022/4/14.
 *
 * @author lan
 */
public class Case1Test extends KafkaTests {

    Producer<String, String> producer;

    static volatile boolean running = true;

    @BeforeEach
    void setUp() {
        producer = newProducer();
    }

    @Test
    void test() throws InterruptedException {
        String topic = "case1_1";
        String group = "case1_1";
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, String>[] consumers = new KafkaConsumer[]{newConsumer(group), newConsumer(group), newConsumer(group)};

        List<String> topicCollection = Collections.singletonList(topic);
        for (int i = 0; i < consumers.length; i++) {
            KafkaConsumer<String, String> consumer = consumers[i];
            consumer.subscribe(topicCollection);
            ConsumerThread consumerThread = new ConsumerThread(consumer, "c-" + i);
            consumerThread.start();
        }

        ProducerThread producerThread = new ProducerThread(producer, topic);
        //producerThread.start();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        running = false;
        System.out.println("exit...");
        TimeUnit.SECONDS.sleep(1);
    }

    static class ProducerThread extends Thread {

        Producer<String, String> producer;
        String topic;

        public ProducerThread(Producer<String, String> producer, String topic) {
            super("producer");
            this.producer = producer;
            this.topic = topic;
        }

        @Override
        public void run() {
            int i = 0;
            while (running) {
                for (int j = 0; j < 6; j++) {
                    producer.send(new ProducerRecord<>(topic, String.valueOf(i++)));
                }
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    static class ConsumerThread extends Thread {

        static Duration duration = Duration.ofMillis(2000);

        KafkaConsumer<String, String> consumer;

        List<String> records = new ArrayList<>(100);

        public ConsumerThread(KafkaConsumer<String, String> consumer, String threadName) {
            super(threadName);
            this.consumer = consumer;
        }

        @Override
        public void run() {
            while (running) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(duration);
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        System.out.println(Thread.currentThread().getName() + ": " + record.value());
                        records.add(record.value());
                    }
                }
            }
            System.out.println(Thread.currentThread().getName() + ": " + records);
        }
    }


    KafkaConsumer<String, String> newConsumer(String group) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", group);
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", deserializer);
        props.setProperty("value.deserializer", deserializer);
        return new KafkaConsumer<>(props);
    }

    KafkaProducer<String, String> newProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);
        return new KafkaProducer<>(props);
    }
}
