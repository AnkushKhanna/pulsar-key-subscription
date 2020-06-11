package producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import shutdown.ShutdownHook;

public class ProducerTrafficUpdated {

    private final List<String> regions = new ArrayList<>();
    final PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
    private final Producer<String> producer = client.newProducer(Schema.STRING)
        .topic("persistent://public/default/region-partitioned")
        .batcherBuilder(BatcherBuilder.KEY_BASED)
        .hashingScheme(HashingScheme.Murmur3_32Hash)
        .create();

    public ProducerTrafficUpdated() throws PulsarClientException {
        regions.add("Berlin");
        regions.add("Hyderabad");
        regions.add("SF");
        regions.add("Dallas");
        regions.add("Delhi");
        regions.add("Calcutta");
        regions.add("New York");
        regions.add("Munich");
        regions.add("London");
        regions.add("Wales");
        regions.add("Hamburg");
        regions.add("Kiev");

        final var shutdownHook = new ShutdownHook(client, producer);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public void produce() throws PulsarClientException, InterruptedException {
        final var random = new Random();
        while (true) {
            final var index = random.nextInt(regions.size());
            final var region = regions.get(index);
            producer.newMessage()
                .key(region)
                .value(region + " has been updated").send();
            System.out.println("Pushed Message for:" + region);
            Thread.sleep(2000);
        }

    }

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        final var producerTraffic = new ProducerTrafficUpdated();
        producerTraffic.produce();
    }
}
