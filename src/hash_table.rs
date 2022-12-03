use std::borrow::Borrow;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// This is a simple hashing function that uses the default hasher,
// It returns three indexes (0..256) consising which are the last 3 bytes of the hash.
// It works on any type that implements Hash.
fn hash_ixs<T>(obj: T) -> (usize, usize, usize)
where
    T: Hash,
{
    let mut hasher = DefaultHasher::new();
    obj.hash(&mut hasher);
    let hash = hasher.finish() as usize;
    (hash % 256, hash / 256 % 256, hash / 256 / 256 % 256)
}

// This is a the KV pair. These are the objects we're going to be storing, looking up, and removing.
struct Pair<K: Eq + Hash, V> {
    key: K,
    value: V,
}

// A very simple hash table implementation.
// It uses a 4D array of Vecs to store the values.
// The first 3 dimensions are the indexes returned by the `hash_ixs` function.
// The last dimension is to handle collisions.
// The size of each dimension is 256, based on the requirement to store 10,000,000 values.
// The closest 2^n above the cube root of 10,000,000 is 256.
// 256^3 = 16,777,216, which is greater than 10,000,000 so if the hash function is sufficiently
// random, we should rarely have collisions. (In theory, I haven't tested this.)
// In terms of memory usage, this is pretty wasteful from the get go, especially if there aren't
// many values, since there's going to be a lot of empty Vecs.
// But, since memory usage is not a requirement, but rather, the retrieval time is, we're going
// to run with this and see where it gets us. (Spoiler: within 10% of `std::collections::HashMap`)
#[derive(Default)]
pub(crate) struct HashTable<K: Eq + Hash, V> {
    #[allow(clippy::type_complexity)]
    pairs: Vec<Vec<Vec<Vec<Pair<K, V>>>>>,
}

impl<K: Eq + Hash, V> HashTable<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            pairs: vec![vec![vec![]]],
        }
    }

    // This is the function that inserts a value into the hash table.
    // It uses the hash function to get the indexes, resizes the vec dimensions if necessary,
    // and then inserts the value into the last dimension, if its key isn't there yet.
    // Vec resize might be a bit slow, but we're optimizing for retrieval time, not insertion time.
    pub(crate) fn insert(&mut self, key: K, value: V) {
        let (ix1, ix2, ix3) = hash_ixs(&key);

        if self.pairs.len() <= ix1 {
            self.pairs.resize_with(ix1 + 1, Vec::new);
        }

        if self.pairs[ix1].len() <= ix2 {
            self.pairs[ix1].resize_with(ix2 + 1, Vec::new);
        }

        if self.pairs[ix1][ix2].len() <= ix3 {
            self.pairs[ix1][ix2].resize_with(ix3 + 1, Vec::new);
        }

        let pairs_vec: &mut Vec<Pair<K, V>> = self.pairs[ix1][ix2][ix3].as_mut();
        let pair = Pair { key, value };
        pairs_vec.retain(|n| n.key != pair.key);
        pairs_vec.push(pair);
    }

    // This is the function that looks up a value in the hash table.
    // It uses the hash function to get the indexes, of the first 3 dimensions,
    // and then iterates over the last dimension to find the value.
    // (Hopefully there shouldn't be more than 1 usually in the 4th dimension.)
    pub(crate) fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let (ix1, ix2, ix3) = hash_ixs(key);

        self.pairs
            .get(ix1)
            .and_then(|v| v.get(ix2))
            .and_then(|v| v.get(ix3))
            .and_then(|v| v.iter().find(|n| n.key.borrow() == key))
            .map(|n| &n.value)
    }

    // This is the function that removes a value from the hash table, if it exists.
    // Find the relevant 4th dimension, and remove the value from it.
    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let (ix1, ix2, ix3) = hash_ixs(key);

        let Some(position) = self
            .pairs
            .get_mut(ix1)
            .and_then(|v| v.get_mut(ix2))
            .and_then(|v| v.get_mut(ix3))
            .and_then(|v| v.iter().position(|n| n.key.borrow() == key))
        else {
            return None
        };

        Some(self.pairs[ix1][ix2][ix3].remove(position).value)
    }
}
