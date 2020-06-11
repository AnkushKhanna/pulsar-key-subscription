package consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

public class AutoSplitConsumer {

    private final Consumer<byte[]> consumer;

    public AutoSplitConsumer(final PulsarClient client,
                             final int partitionIndex) throws PulsarClientException {
        consumer = client.newConsumer()
            .subscriptionMode(SubscriptionMode.Durable)
            .topic("persistent://public/default/region-partitioned")
            .consumerName("Region Consumer: " + partitionIndex)
            .subscriptionName("regions-subscription-hashed")
            .subscriptionType(SubscriptionType.Key_Shared)
            .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
            .subscribe();

    }

    public void consume() throws PulsarClientException {
        try {
            while (true) {
                final var message = consumer.receive();
                System.out.println("Key of the message");
                System.out.println(message.getKey());
                consumer.acknowledge(message);
            }
        } finally {
            System.out.println("Closing consumer");
            consumer.close();
        }
    }

    public static void main(String[] args) throws PulsarClientException {
        final int partitionIndex = Integer.parseInt(args[0]);
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
        System.out.println("Application Terminating ...");

        final var stickyConsumer = new AutoSplitConsumer(client, partitionIndex);
        stickyConsumer.consume();
    }

}

