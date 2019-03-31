package biz.lwb.tut;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


public class CuratorTutorialTest extends BaseManualTest {

    private static final String KEY_FORMAT = "/%s";

    @Test
    public void givenPath_whenCreateKey_thenValueIsStored() throws Exception {
        try (CuratorFramework client = newClient()) {
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            String key = getKey();
            String expected = "my_value";

            // Create key nodes structure
            client.create().forPath(key);

            // Set data value for our key
            async.setData().forPath(key, expected.getBytes());

            // Get data value
            AtomicBoolean isEquals = new AtomicBoolean();
            async.getData()
                    .forPath(key)
                    .thenAccept(
                            data -> isEquals.set(new String(data).equals(expected)));

            await().until(() -> {
                boolean result = isEquals.get();
                assertThat(result).isTrue();
                return result;
            });
        }
    }

    @Test
    public void givenPath_whenWatchAKeyAndStoreAValue_thenWatcherIsTriggered() {
        try (CuratorFramework client = newClient()) {
            client.start();
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            String key = getKey();
            String expected = "my_value";

            // Create key structure
            async.create().forPath(key);

            List<String> changes = new ArrayList<>();

            // Watch data value
            async.watched().getData().forPath(key).event()
                    .thenAccept(watchedEvent -> {
                        try {
                            changes.add(new String(client.getData().forPath(watchedEvent.getPath())));
                        } catch (Exception e) {
                            // fail ...
                        }
                    });

            // Set data value for our key
            async.setData().forPath(key, expected.getBytes());

            await().until(() -> {
                int size = changes.size();
                boolean actual = size > 0;
                assertThat(actual).isTrue();
                return actual;
            });
        }
    }

    @Test
    public void testCache() throws Exception {
        CuratorFramework client = newClient();
        client.getUnhandledErrorListenable().addListener((message, e) -> {
            System.err.println("error=" + message);
            e.printStackTrace();
        });
        client.getConnectionStateListenable().addListener((c, newState) -> {
            System.out.println("state=" + newState);
        });
        client.start();

        TreeCache cache = TreeCache.newBuilder(client, "/").setCacheData(false).build();
        cache.getListenable().addListener((c, event) -> {
            if ( event.getData() != null )
            {
                System.out.println("type=" + event.getType() + " path=" + event.getData().getPath());
            }
            else
            {
                System.out.println("type=" + event.getType());
            }
        });
        cache.start();

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        in.readLine();
    }

    private String getKey() {
        return String.format(KEY_FORMAT, UUID.randomUUID()
                .toString());
    }
}