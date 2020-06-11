import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.util.Hash;
import org.apache.pulsar.common.util.Murmur3_32Hash;

public class Main {

    public static int ROUNDS = 100;

    static class PartitionHash {
        public int partitionIndex;
        public int hash;

        public PartitionHash(final int partitionIndex, final int hash) {
            this.partitionIndex = partitionIndex;
            this.hash = hash;
        }
    }

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        System.out.println("Starting");
        final NavigableMap<Integer, Integer> partitionHashes = new TreeMap<>();
        final var partitions = 4;
        final var ringCount = 50;
        for (int i = 0; i < ringCount; i++) {
            for (int j = 0; j < partitions; j++) {
                var hash = Murmur3_32Hash.getInstance().makeHash((i + "_" + j).getBytes()) % 65536;
                partitionHashes.put(hash, j);
            }
        }
        var lastEntry = partitionHashes.lastEntry();
        if (lastEntry.getKey() != 65535) {
            partitionHashes.put(65535, partitions - 1);
        }

        Map<Integer, List<Range>> partitionRanges = new HashMap<>();

        for (final int rangeEnd : partitionHashes.keySet()) {
            final var partition = partitionHashes.get(rangeEnd);
            final var ceilingEntry = partitionHashes.lowerEntry(rangeEnd);
            var rangeStart = 0;
            if (ceilingEntry != null) {
                rangeStart = ceilingEntry.getKey() + 1;
            }
            if (rangeStart <= rangeEnd) {
                final var list = partitionRanges.getOrDefault(partition, new ArrayList<>());
                list.add(Range.of(rangeStart, rangeEnd));
                partitionRanges.put(partition, list);
            }
        }

        partitionRanges.forEach((k, v) -> {
            System.out.println("Key:" + k);
            System.out.println("Range");
            System.out.println(v);
            int sum = v.stream().map(r -> (r.getEnd() - r.getStart())).mapToInt(x -> x).sum();
            System.out.println(sum);
        });
        var totalSum = partitionRanges.entrySet().stream().flatMap(e -> e.getValue().stream()).mapToInt(r -> r.getEnd()-r.getStart()+1).sum();
        System.out.println(totalSum);
        //        PulsarClient client = PulsarClient.builder()
        //            .serviceUrl("pulsar://localhost:6650")
        //            .build();
        //
        //        final Producer<String> producer = client.newProducer(Schema.STRING)
        //            .topic("my-topic1")
        //            .create();
        //
        //        final Consumer<String> consumer = client.newConsumer(Schema.STRING)
        //            .topic("my-topic1")
        //            .subscriptionName("my-subscription1")
        //            .subscribe();
        //
        //
        //        final ExecutorService executors = Executors.newFixedThreadPool(2);
        //
        //        final CountDownLatch latch = new CountDownLatch(2);
        //        executors.execute(() -> {
        //            try {
        //                for (int i = 0; i < ROUNDS; i++) {
        //                    System.out.println("starting consumption");
        //                    final Message<String> message = consumer.receive();
        //
        //                    System.out.println(message.getKey());
        //                    System.out.println(message.getValue());
        //                    consumer.acknowledge(message);
        //                }
        //                latch.countDown();
        //            } catch (PulsarClientException e) {
        //                e.printStackTrace();
        //            }
        //        });
        //
        //        executors.execute(() -> {
        //            for (int i = 0; i < ROUNDS; i++) {
        //                try {
        //                    System.out.println("starting production");
        //                    producer.newMessage().key("test1").value("My message").send();
        //                    System.out.println("Message send: " + i);
        //                } catch (PulsarClientException e) {
        //                    e.printStackTrace();
        //                }
        //            }
        //            latch.countDown();
        //        });
        //
        //        latch.await();
        //        executors.shutdownNow();
        //
        //        System.out.println("CLOSING");
        //        producer.close();
        //        consumer.close();
        //        client.close();
    }
}
