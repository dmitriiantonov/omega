#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use omega_cache::core::backoff::BackoffPolicy;
    use omega_cache::core::engine::CacheEngine;
    use omega_cache::core::workload::WorkloadGenerator;
    use omega_cache_macros::cache;
    use std::thread::scope;

    #[test]
    fn test_performance_clock() {
        let capacity = 10000;
        let cache = cache! {
            engine: Clock {
                capacity: capacity,
                metrics: { shards: 4, latency_samples: 1024 }
            },
            backoff: { policy: BackoffPolicy::Linear, limit: 10 }
        };

        run_workload(cache, 16, 1000000, 100000, 1.3, "Clock");
    }

    #[test]
    fn test_performance_s3fifo() {
        let capacity = 10000;
        let cache = cache! {
            engine: S3FIFO {
                capacity: capacity,
                metrics: { shards: 16, latency_samples: 1024 }
            },
            backoff: { policy: BackoffPolicy::Exponential, limit: 10 }
        };

        run_workload(cache, 16, 1000000, 100000, 1.3, "S3FIFO");
    }

    fn run_workload<E>(
        cache: omega_cache::Cache<E, Bytes, &'static str>,
        num_threads: usize,
        ops_per_thread: usize,
        num_keys: usize,
        skew: f64,
        label: &str,
    ) where
        E: CacheEngine<Bytes, &'static str> + Send + Sync,
    {
        let workload_generator = WorkloadGenerator::new(num_keys, skew);
        let total_ops = num_threads * ops_per_thread;

        let mut warm_rng = rand::rng();

        for _ in 0..num_keys {
            let key = workload_generator.key(&mut warm_rng);
            cache.insert(key, "warmup");
        }

        println!("--- Testing: {label} ---");
        let start = std::time::Instant::now();

        scope(|scope| {
            for _ in 0..num_threads {
                scope.spawn(|| {
                    let mut thread_rng = rand::rng();
                    for op in 0..ops_per_thread {
                        let key = workload_generator.key(&mut thread_rng);

                        if op % 5 == 0 {
                            cache.insert(key, "value");
                        } else {
                            let _ = cache.get(key.as_ref());
                        }
                    }
                });
            }
        });

        let duration = start.elapsed();
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        let metrics = cache.metrics();

        println!("Execution Time:  {:?}", duration);
        println!("Throughput:      {:.2} ops/sec", ops_per_sec);
        println!("Hit Rate:        {:.2}%", metrics.hit_rate() * 100.0);
    }
}
