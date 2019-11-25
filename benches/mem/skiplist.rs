use criterion::{BatchSize, Bencher, BenchmarkId, Criterion};
use std::sync::Arc;
use wickdb::mem::arena::BlockArena;
use wickdb::mem::skiplist::*;
use wickdb::BytewiseComparator;

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("Skiplist::insert");
    let cmp = Arc::new(BytewiseComparator::new());
    let key_len = vec![1, 100, 500, 1000, 4000, 10000, 100000];
    for len in key_len {
        group.bench_with_input(
            BenchmarkId::from_parameter(len),
            &len,
            |b: &mut Bencher, length| {
                b.iter_batched(
                    || {
                        (
                            Skiplist::new(cmp.clone(), Box::new(BlockArena::new())),
                            vec![0u8; *length],
                        )
                    },
                    |(s, key)| s.insert(key),
                    BatchSize::SmallInput,
                )
            },
        );
    }
}

pub fn bench_skiplist(c: &mut Criterion) {
    bench_insert(c);
}
