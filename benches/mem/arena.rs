use criterion::{BatchSize, Bencher, BenchmarkId, Criterion};
use wickdb::mem::arena::*;

static CHUNK_SIZE: [usize; 6] = [256, 1024, 4096, 10000, 16384, 33333];

fn bench_allocate(c: &mut Criterion) {
    let mut group = c.benchmark_group("BlockArena::allocate");
    for size in CHUNK_SIZE.clone().into_iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b: &mut Bencher, size| {
                b.iter_batched(
                    || BlockArena::new(),
                    |arena| arena.allocate(*size),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn bench_allocate_aligned(c: &mut Criterion) {
    let mut group = c.benchmark_group("BlockArena::allocate_aligned");
    for size in CHUNK_SIZE.clone().into_iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b: &mut Bencher, size| {
                b.iter_batched(
                    || BlockArena::new(),
                    |arena| arena.allocate_aligned(*size),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

pub fn bench_arena(c: &mut Criterion) {
    bench_allocate(c);
    bench_allocate_aligned(c);
}
