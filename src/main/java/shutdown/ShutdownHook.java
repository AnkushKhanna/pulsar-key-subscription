package shutdown;

import java.io.Closeable;
import java.io.IOException;
import org.apache.pulsar.client.api.PulsarClient;

public class ShutdownHook extends Thread {

    private final PulsarClient client;
    private final Closeable closeable;

    public ShutdownHook(final PulsarClient client, final Closeable closeable) {
        this.client = client;
        this.closeable = closeable;
    }

    @Override
    public void run() {
        System.out.println("Performing shutdown");
        try {
            closeable.close();
            client.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
