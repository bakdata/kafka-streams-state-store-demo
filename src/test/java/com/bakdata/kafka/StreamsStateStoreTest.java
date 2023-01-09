package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class StreamsStateStoreTest {

    private static final String INPUT = "input";
    private static final String SUMS = "sums";
    private static final String OUTPUT = "output";
    @Container
    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"));

    static Stream<Arguments> generateMappers() {
        return Stream.of(
                Arguments.of((SumFunction) (streamsBuilder, sumsStore, input) -> {
                    streamsBuilder.addStateStore(sumsStore);
                    return input.transformValues(SumTransformer::new, sumsStore.name());
                }),
                Arguments.of((SumFunction) (streamsBuilder, sumsStore, input) -> input.transformValues(
                        new ValueTransformerWithKeySupplier<String, Integer, Long>() {
                            @Override
                            public Set<StoreBuilder<?>> stores() {
                                return Set.of(sumsStore);
                            }

                            @Override
                            public ValueTransformerWithKey<String, Integer, Long> get() {
                                return new SumTransformer();
                            }
                        })),
                Arguments.of((SumFunction) (streamsBuilder, sumsStore, input) -> {
                    streamsBuilder.addStateStore(sumsStore);
                    return input.processValues(SumProcessor::new, sumsStore.name());
                }),
                Arguments.of((SumFunction) (streamsBuilder, sumsStore, input) -> input.processValues(
                        new FixedKeyProcessorSupplier<String, Integer, Long>() {
                            @Override
                            public Set<StoreBuilder<?>> stores() {
                                return Set.of(sumsStore);
                            }

                            @Override
                            public FixedKeyProcessor<String, Integer, Long> get() {
                                return new SumProcessor();
                            }
                        }))
        );
    }

    private static StoreBuilder<KeyValueStore<String, Long>> createStore() {
        final KeyValueBytesStoreSupplier sumsStoreSupplier = Stores.inMemoryKeyValueStore(SUMS);
        return Stores.keyValueStoreBuilder(sumsStoreSupplier, Serdes.String(), Serdes.Long());
    }

    @ParameterizedTest
    @MethodSource("generateMappers")
    void shouldSum(final SumFunction sumFunction)
            throws ExecutionException, InterruptedException, TimeoutException {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final StoreBuilder<KeyValueStore<String, Long>> sumsStore = createStore();
        final KStream<String, Integer> input =
                streamsBuilder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Long> sums = sumFunction.sum(streamsBuilder, sumsStore, input);
        sums.to(OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));
        final Topology topology = streamsBuilder.build();

        try (final Admin adminClient = this.createAdminClient()) {
            adminClient.createTopics(List.of(new NewTopic(INPUT, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }

        try (final KafkaStreams kafkaStreams = this.createStreams(topology)) {
            kafkaStreams.start();

            try (final Producer<String, Integer> producer = this.createProducer()) {
                producer.send(new ProducerRecord<>(INPUT, "foo", 2));
                producer.flush();
            }

            try (final Consumer<String, Long> consumer = this.createConsumer()) {
                final ConsumerRecords<String, Long> records = consumer.poll(Duration.ofSeconds(10));
                assertThat(Lists.newArrayList(records))
                        .hasSize(1)
                        .anySatisfy(record -> {
                            assertThat(record.key()).isEqualTo("foo");
                            assertThat(record.value()).isEqualTo(2);
                        });
            }
        }
    }

    private KafkaStreams createStreams(final Topology topology) {
        final Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafka.getBootstrapServers());
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
        return new KafkaStreams(topology, streamsProperties);
    }

    private Consumer<String, Long> createConsumer() {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        final Consumer<String, Long> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of(OUTPUT));
        return consumer;
    }

    private KafkaProducer<String, Integer> createProducer() {
        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafka.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        return new KafkaProducer<>(producerProperties);
    }

    private AdminClient createAdminClient() {
        final Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafka.getBootstrapServers());
        return AdminClient.create(adminProperties);
    }

    @FunctionalInterface
    private interface SumFunction {
        KStream<String, Long> sum(StreamsBuilder streamsBuilder, StoreBuilder<KeyValueStore<String, Long>> sumsStore,
                KStream<String, Integer> input);
    }

    private static class SumTransformer implements ValueTransformerWithKey<String, Integer, Long> {
        private KeyValueStore<String, Long> sums;

        @Override
        public void init(final ProcessorContext context) {
            this.sums = context.getStateStore(SUMS);
        }

        @Override
        public Long transform(final String key, final Integer value) {
            final long oldSum = Optional.ofNullable(this.sums.get(key))
                    .orElse(0L);
            final long newSum = oldSum + value;
            this.sums.put(key, newSum);
            return newSum;
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    private static class SumProcessor implements FixedKeyProcessor<String, Integer, Long> {
        private KeyValueStore<String, Long> sums;
        private FixedKeyProcessorContext<String, Long> context;

        @Override
        public void init(final FixedKeyProcessorContext<String, Long> context) {
            this.context = context;
            this.sums = context.getStateStore(SUMS);
        }

        @Override
        public void process(final FixedKeyRecord<String, Integer> record) {
            // throws error because this.sums is null
            final long oldSum = Optional.ofNullable(this.sums.get(record.key()))
                    .orElse(0L);
            final long newSum = oldSum + 1;
            this.sums.put(record.key(), newSum);
            this.context.forward(record.withValue(newSum));
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
