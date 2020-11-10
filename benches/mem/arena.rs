use criterion::{BatchSize, Bencher, BenchmarkId, Criterion};
use wickdb::mem::arena::*;

static CHUNK_SIZE: [usize; 6] = [256, 1024, 4096, 10000, 16384, 33333];

fn bench_block_arena_allocate(c: &mut Criterion) {
    let mut group = c.benchmark_group("BlockArena::allocate");
    for size in CHUNK_SIZE.clone().iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b: &mut Bencher, size| {
                b.iter_batched(
                    || BlockArena::default(),
                    |arena| unsafe { arena.allocate::<u8>(*size, 8) },
                    BatchSize::PerIteration,
                );
            },
        );
    }
}

fn bench_offset_arena_allocate(c: &mut Criterion) {
    let mut group = c.benchmark_group("OffsetArena::allocate");
    for size in CHUNK_SIZE.clone().iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b: &mut Bencher, size| {
                b.iter_batched(
                    || OffsetArena::with_capacity(1 << 10),
                    |arena| unsafe { arena.allocate::<u8>(*size, 8) },
                    BatchSize::PerIteration,
                );
            },
        );
    }
}

pub fn bench_arena(c: &mut Criterion) {
    bench_block_arena_allocate(c);
    bench_offset_arena_allocate(c);
}
