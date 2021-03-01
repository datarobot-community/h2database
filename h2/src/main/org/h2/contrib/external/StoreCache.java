package org.h2.contrib.external;

import org.h2.mvstore.MVStore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Share instances of MVStore between different connections
 */
public class StoreCache {
// ------------------------------ FIELDS ------------------------------

    static final Map<String, MVStore> storeCache = new HashMap<String, MVStore>();
    static final Map<String, AtomicInteger> storeCacheCount = new HashMap<String, AtomicInteger>();

// -------------------------- STATIC METHODS --------------------------

    public static void close(MVStore indexStore, String fileName) {
        synchronized (storeCache) {
            int count = storeCacheCount.get(fileName).getAndDecrement();
            if (count == 1) {
                indexStore.close();
                storeCache.remove(fileName);
            }
        }
    }

    public static MVStore open(String indexFileName, boolean readOnly) {
        synchronized (storeCache) {
            if (storeCache.containsKey(indexFileName)) {
                storeCacheCount.get(indexFileName).getAndIncrement();
                return storeCache.get(indexFileName);
            } else {
                MVStore.Builder builder = new MVStore.Builder().fileName(indexFileName);
                if (readOnly)
                    builder.readOnly();
                MVStore store = builder.open();
                storeCache.put(indexFileName, store);
                storeCacheCount.put(indexFileName, new AtomicInteger(1));
                return store;
            }
        }
    }
}
