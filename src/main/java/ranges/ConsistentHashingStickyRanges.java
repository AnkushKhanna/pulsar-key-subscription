package ranges;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.util.Murmur3_32Hash;

public class ConsistentHashingStickyRanges {

    public Map<Integer, List<Range>> getRange(final int partitionCount, final int numberOfPoints) {
        return createRanges(partitionCount, numberOfPoints);
    }

    public List<Range> getRange(final int nodeIndex,
                                final int nodeCount,
                                final int numberOfPoints) {
        return createRanges(nodeCount, numberOfPoints).get(nodeIndex);
    }

    public double calcPercentage(final List<Range> ranges) {
        return (ranges.stream().mapToInt(r -> r.getEnd()-r.getStart() +1).sum() / (double) KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE) * 100;
    }

    private Map<Integer, List<Range>> createRanges(final int nodeCount,
                                                   final int numberOfPoints) {
        final NavigableMap<Integer, Integer> partitionHashes = new TreeMap<>();
        for (int i = 0; i < numberOfPoints; i++) {
            for (int j = 0; j < nodeCount; j++) {
                final var hash = Murmur3_32Hash.getInstance().makeHash((i + "_" + j).getBytes()) % KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE;
                partitionHashes.put(hash, j);
            }
        }
        final var lastEntry = partitionHashes.lastEntry();
        if (lastEntry.getKey() != KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE - 1) {
            partitionHashes.put(KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE - 1, nodeCount - 1);
        }

        final Map<Integer, List<Range>> partitionRanges = new HashMap<>();

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

        return partitionRanges;
    }

}
