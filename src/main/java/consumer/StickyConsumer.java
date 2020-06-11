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

public class StickyConsumer {
    private final Consumer<String> consumer;

    public StickyConsumer(final PulsarClient client,
                          final List<Range> ranges,
                          final int partitionIndex) throws PulsarClientException {
        consumer = client.newConsumer(Schema.STRING)
            .subscriptionMode(SubscriptionMode.Durable)
            .topic("persistent://public/default/region-partitioned")
            .consumerName("Region Consumer: " + partitionIndex)
            .subscriptionName("regions-subscription-sticky-hashed")
            .subscriptionType(SubscriptionType.Key_Shared)
            .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(ranges.toArray(new Range[ranges.size()])))
            .subscribe();

    }

    public void consume() throws PulsarClientException {
        try {
            while (true) {
                final var message = consumer.receive();
                System.out.println(message.getKey());
                System.out.println(consumer.getStats().getTotalAcksSent());
                consumer.acknowledge(message);
            }
        } finally {
            System.out.println("Closing consumer");
            consumer.close();
        }
    }

    public static void main(String[] args) throws PulsarClientException {
        final var partitionIndex = Integer.parseInt(args[0]);
        final var partitionCount = Integer.parseInt(args[1]);
        final var numberOfPoints = Integer.parseInt(args[2]);

        final var consistentHashingRanges = new ConsistentHashingStickyRanges();

        final var ranges = consistentHashingRanges.getRange(partitionIndex, partitionCount, numberOfPoints);
        //
        //        final var ranges = new ArrayList<Range>();
        ////        final var range = Range.of(hashRangeSize * partitionIndex, hashRangeSize * (partitionIndex + 1) - 1);
        ////        ranges.add(Range.of(0,99));
        ////        ranges.add(Range.of(400, 65535));
        //        ranges.add(Range.of(100, 399));
        System.out.println(ranges);
        System.out.println(consistentHashingRanges.calcPercentage(ranges));
        final PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown Hook is running !");
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }));

        final var stickyConsumer = new StickyConsumer(client, ranges, partitionIndex);
        stickyConsumer.consume();
    }
}
