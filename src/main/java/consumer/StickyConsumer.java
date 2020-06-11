package consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import shutdown.ShutdownHook;

public class StickyConsumer {
    final PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

    private final Consumer<String> consumer;

    public StickyConsumer(final Range range,
                          final int nodeIndex) throws PulsarClientException {
        consumer = client.newConsumer(Schema.STRING)
            .subscriptionMode(SubscriptionMode.Durable)
            .topic("persistent://public/default/region-partitioned")
            .consumerName("Region Consumer: " + nodeIndex)
            .subscriptionName("regions-subscription-sticky-hashed")
            .subscriptionType(SubscriptionType.Key_Shared)
            .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(range))
            .subscribe();

        final var shutdownHook = new ShutdownHook(client, consumer);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public void consume() throws PulsarClientException {
        while (true) {
            final var message = consumer.receive();
            System.out.println(message.getKey());
            System.out.println(consumer.getStats().getTotalAcksSent());
            consumer.acknowledge(message);
        }
    }

    public static void main(String[] args) throws PulsarClientException {
        final var nodeIndex = Integer.parseInt(args[0]);
        final var nodeCount = Integer.parseInt(args[1]);

        final var size = KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE / nodeCount;

        final var range = Range.of(nodeIndex * size, (nodeIndex + 1) * size - 1);

        final var stickyConsumer = new StickyConsumer(range, nodeIndex);

        stickyConsumer.consume();
    }
}
