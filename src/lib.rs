// use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

mod hash_table;
use hash_table::HashTable;

// This is the Cache.
// - `Arc` makes it possible to clone and share it between threads.
// - `RwLock` is required for mutability between threads. Since I'm optimizing for retrieval time,
//   I chose it over `Mutex`.
// - `HashTable` is the custom hash table that I wrote. However, this can be replaced in-place
//   with `std::collections::HashMap` and it works just fine. (Even a bit faster)
// - The key for the hash table (`Arc<K>`) can be anything that implements `Eq` and `Hash`.
//   It's wrapped in `Arc` because I need to pass it to the future that handles the expiration.
// - The value of the hash table is a tuple of the value (`Arc<V>`) and the optional handle for the expiration
//   handler task. `V` is wrapped in `Arc` so you can clone it and share it between threads.
//   The expiration task handle needs to be there in case of a race in which the expiration handler is called
//   for a key that has been since removed and replaced with a new value.
// - The whole expiration business assumes we're running in a `tokio` runtime. Would panic otherwise.
#[derive(Clone, Default)]
pub struct Cache<K: Eq + Hash, V> {
    inner: Arc<RwLock<HashTable<Key<K>, Value<V>>>>,
}
type Key<K> = Arc<K>;
type Value<V> = (Arc<V>, Option<JoinHandle<()>>);

impl<K: Eq + Hash, V> Cache<K, V> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashTable::new())),
        }
    }

    // Get a value from the cache.
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let lock = self.inner.read();
        let v = lock.get(key);
        v.map(|v| Arc::clone(&v.0))
    }

    // Insert a value into the cache.
    pub fn insert(&mut self, key: K, value: V) {
        self.inner
            .write()
            .insert(Arc::new(key), (Arc::new(value), None));
    }

    // Insert a value into the cache with an expiration time.
    pub fn insert_with_expiry(&mut self, key: K, value: V, expiry: Duration)
    where
        K: 'static,
        V: 'static,
        Key<K>: Send + Sync,
        Value<V>: Send + Sync,
    {
        let key = Arc::new(key);

        // Spawn a task that will remove the key from the cache after the specified duration.
        let expired_key = Arc::clone(&key);
        let inner = Arc::clone(&self.inner);
        let handle = spawn(async move {
            sleep(expiry).await;
            inner.write().remove(&expired_key);
        });

        // Insert the key into the cache with the handle to the expiration task.
        self.inner
            .write()
            .insert(key, (Arc::new(value), Some(handle)));
    }

    // Remove a value from the cache.
    pub fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        // Remove the key from the cache (if exists) ...
        let Some((value, handle)) = self.inner.write().remove(key) else {
            return None
        };

        // ... and cancel the expiration task if it exists. Otherwise you're gonna have a bad time.
        if let Some(handle) = handle {
            println!("Cancelling");
            handle.abort();
        }

        // Return the value previosly associated with the key.
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;
    use uuid::Uuid;

    // Simple test to make sure the cache works (insert and get).
    #[test]
    fn insert_get() {
        let mut cache = Cache::new();
        cache.insert("key", "value");
        assert_eq!(cache.get(&"key"), Some(Arc::new("value")));
    }

    // Simple test to make sure the cache works (insert, get, remove).
    #[test]
    fn insert_get_remove() {
        let mut cache = Cache::new();
        cache.insert("key", "value");
        assert_eq!(cache.get(&"key"), Some(Arc::new("value")));
        cache.remove(&"key");
        assert_eq!(cache.get(&"key"), None);
    }

    // Test expiration.
    // This test will start a `tokio` runtime and spawn a task that will insert a key into the cache
    // with an expiration time of 1 second.
    // It will immediately try to get the value from the cache and assert that it's there.
    // Then it will wait for 2 seconds and check that the key has been removed from the cache.
    #[test]
    fn expiring_kv() {
        // Start a tokio runtime.
        let rt = Runtime::new().unwrap();
        let _guard = rt.enter();

        let mut cache = Cache::new();

        // Insert a key into the cache with an expiration time of 1 second, and make sure it's there.
        cache.insert_with_expiry("key", "value", Duration::from_secs(1));
        assert_eq!(cache.get(&"key"), Some(Arc::new("value")));

        // Wait for 2 seconds and make sure the key has been removed from the cache.
        ::std::thread::sleep(Duration::from_secs(2));
        assert_eq!(cache.get(&"key"), None);
    }

    // Testing single-threaded perfomance under design capacity.
    // Keys are UUIDs for maximum randomness.
    // Loads 10,000,000 keys into the cache and then gets them all.
    // Measures the time it retrieve each key.
    // Tests the percentage of retrievals under 1ms (should be >95%)
    // Tests the percentage of retrievals under 5ms (should be >99%)
    // (Passed with flying colors on an Apple M1 Pro)
    #[test]
    fn perf() {
        let mut cache = Cache::new();
        println!("Loading...");
        let mut uuids = Vec::new();

        // Load 10,000,000 KV pairs into the cache, and record the keys for later retrieval.
        for i in 0..10_000_000 {
            let uuid = Uuid::new_v4();
            cache.insert(uuid, i);
            uuids.push(uuid);
        }

        // Initialize the "histogram" for the percentiles that we're going to measure.
        let mut under_1ms = 0;
        let mut under_5ms = 0;
        let mut total_time = Duration::from_millis(0);

        // Get each key from the cache and measure the time it takes.
        for uuid in uuids {
            let start = ::std::time::Instant::now();
            cache.get(&uuid);
            let elapsed = start.elapsed();
            if elapsed < Duration::from_millis(1) {
                under_1ms += 1;
            }
            if elapsed < Duration::from_millis(5) {
                under_5ms += 1;
            }
            total_time += elapsed;
        }

        // Calculate the results
        let under_1ms_pct = under_1ms as f64 / 10_000_000.0 * 100.0;
        let under_5ms_pct = under_5ms as f64 / 10_000_000.0 * 100.0;
        assert!(under_1ms_pct > 95.0);
        assert!(under_5ms_pct > 99.0);
        println!("Under 1ms: {}%", under_1ms_pct);
        println!("Under 5ms: {}%", under_5ms_pct);
        println!(
            "Average: {}ms",
            total_time.as_millis() as f64 / 10_000_000.0
        );
    }

    // Same as the previous test but with 10 threads reading in parallel.
    #[test]
    fn threads() {
        // Initialize counters and cache
        let under_1ms = Arc::new(::std::sync::atomic::AtomicUsize::new(0));
        let under_5ms = Arc::new(::std::sync::atomic::AtomicUsize::new(0));
        let mut cache = Cache::new();

        // Fill the cache with 10,000,000 KV pairs.
        let mut uuids = Vec::new();
        for i in 0..10_000_000 {
            let uuid = Uuid::new_v4();
            cache.insert(uuid, i);
            uuids.push(uuid);
        }

        // Initialize the threads
        let uuids = Arc::new(uuids);
        let mut threads = Vec::new();
        for i in 0..10 {
            let cache = cache.clone();
            let under_1ms = Arc::clone(&under_1ms);
            let under_5ms = Arc::clone(&under_5ms);
            let uuids = Arc::clone(&uuids);

            // Spawn the threads and calculate the time it takes to get each key.
            threads.push(::std::thread::spawn(move || {
                for uuid in uuids.iter().skip(i * 1_000_000).take(1_000_000) {
                    let start = ::std::time::Instant::now();
                    cache.get(&uuid);
                    let elapsed = start.elapsed();
                    if elapsed < Duration::from_millis(1) {
                        under_1ms.fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
                    }
                    if elapsed < Duration::from_millis(5) {
                        under_5ms.fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }));
        }

        // Wait for the threads to finish.
        for thread in threads {
            thread.join().unwrap();
        }

        // Calculate the results
        let under_1ms_pct =
            under_1ms.load(::std::sync::atomic::Ordering::Relaxed) as f64 / 10_000_000.0 * 100.0;
        let under_5ms_pct =
            under_5ms.load(::std::sync::atomic::Ordering::Relaxed) as f64 / 10_000_000.0 * 100.0;
        println!("Under 1ms: {}%", under_1ms_pct);
        println!("Under 5ms: {}%", under_5ms_pct);
        assert!(under_1ms_pct > 95.0);
        assert!(under_5ms_pct > 99.0);
    }
}
