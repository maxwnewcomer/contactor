use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use sqids::Sqids;

pub struct IdFactory {
    sqids: Sqids,
    internal_count: AtomicU64,
}

impl Debug for IdFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdFactory")
            .field("internal_count", &self.internal_count)
            .finish()
    }
}

impl IdFactory {
    pub fn new() -> Self {
        let mut rng = SmallRng::from_entropy();

        IdFactory {
            sqids: Sqids::builder()
                .min_length(5)
                .build()
                .expect("failed to build id factory"),
            internal_count: AtomicU64::new(rng.next_u64()),
        }
    }

    pub fn gen_id(&self) -> String {
        self.sqids
            .encode(&[self.internal_count.fetch_add(1, Ordering::SeqCst)])
            .expect("ran out of sqids")
    }
}
