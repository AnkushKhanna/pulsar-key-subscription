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
import ranges.KnownKeysStickyRange;
import shutdown.ShutdownHook;
import utils.Regions;

public class StickyConsumerKnownKey {

    private final PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

    private final Consumer<String> consumer;

    public StickyConsumerKnownKey(final List<Range> ranges,
                                  final int nodeIndex) throws PulsarClientException {
        consumer = client.newConsumer(Schema.STRING)
            .subscriptionMode(SubscriptionMode.Durable)
            .topic("persistent://public/default/region-partitioned")
            .consumerName("Region Consumer: " + nodeIndex)
            .subscriptionName("regions-subscription-known-keys-sticky-hashed")
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

        final List<String> regions = Regions.getRegions();
        final var sizePerNode = (int) Math.ceil(regions.size() / (double) nodeCount);

        final KnownKeysStickyRange stickyRanges = new KnownKeysStickyRange();

        final var ranges = stickyRanges.getRange(regions, nodeIndex, sizePerNode, nodeCount);

        final StickyConsumerKnownKey consumer = new StickyConsumerKnownKey(ranges, nodeIndex);
        consumer.consume();
    }
}
