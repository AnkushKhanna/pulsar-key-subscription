package ranges;

import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.impl.Murmur3_32Hash;

public class KnownKeysStickyRange {

    public List<Range> getRange(final List<String> keys, final int nodeIndex, final int sizePerNode, final int totalNodes) {
        if (totalNodes * sizePerNode < keys.size()) {
            throw new RuntimeException("Nodes are less, please provide more nodes");
        }

        final int startIndex = nodeIndex * sizePerNode;
        final int endIndex = (nodeIndex + 1) * sizePerNode;

        final List<Range> ranges = new ArrayList<>();
        for (int i = startIndex; i < endIndex; i++) {
            System.out.println("Key: " + keys.get(i));
            final int hash = Murmur3_32Hash.getInstance().makeHash(keys.get(i)) % 65536;
            System.out.println("Hash: " + hash);
            ranges.add(Range.of(hash, hash));
        }

        return ranges;
    }

}
