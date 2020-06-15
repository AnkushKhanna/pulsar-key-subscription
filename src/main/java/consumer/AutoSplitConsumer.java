package consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import shutdown.ShutdownHook;

public class AutoSplitConsumer {
    private final PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

    private final Consumer<String> consumer = client.newConsumer(Schema.STRING)
        .subscriptionMode(SubscriptionMode.Durable)
        .topic("persistent://public/default/region-partitioned")
        .consumerName("Region Consumer")
        .subscriptionName("regions-subscription-hashed")
        .subscriptionType(SubscriptionType.Key_Shared)
        .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
        .subscribe();

    public void consume() throws PulsarClientException {
        while (true) {
            final var message = consumer.receive();
            System.out.println("Key of the message");
            System.out.println(message.getKey());
            consumer.acknowledge(message);
        }
    }

    public AutoSplitConsumer() throws PulsarClientException {
        final var shutdownHook = new ShutdownHook(client, consumer);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public static void main(String[] args) throws PulsarClientException {

        final var stickyConsumer = new AutoSplitConsumer();
        stickyConsumer.consume();
    }

}

