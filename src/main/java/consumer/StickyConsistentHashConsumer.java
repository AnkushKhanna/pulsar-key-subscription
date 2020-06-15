package consumer;

import java.util.List;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import ranges.ConsistentHashingStickyRanges;
import shutdown.ShutdownHook;

public class StickyConsistentHashConsumer {
    private final PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

    private final Consumer<String> consumer;

    public StickyConsistentHashConsumer(final List<Range> ranges,
                                        final int partitionIndex) throws PulsarClientException {
        consumer = client.newConsumer(Schema.STRING)
            .subscriptionMode(SubscriptionMode.Durable)
            .topic("persistent://public/default/region-partitioned")
            .consumerName("Region Consumer: " + partitionIndex)
            .subscriptionName("regions-subscription-sticky-hashed")
            .subscriptionType(SubscriptionType.Key_Shared)
            .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(ranges.toArray(new Range[ranges.size()])))
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
        final var numberOfPoints = Integer.parseInt(args[2]);

        final var consistentHashingRanges = new ConsistentHashingStickyRanges();

        final var ranges = consistentHashingRanges.getRange(nodeIndex, nodeCount, numberOfPoints);

        System.out.println(consistentHashingRanges.calcPercentage(ranges));

        final var stickyConsumer = new StickyConsistentHashConsumer(ranges, nodeIndex);

        stickyConsumer.consume();
    }
}
