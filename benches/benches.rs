use criterion::Criterion;
use std::time::Duration;

mod mem;
fn main() {
    let mut c = Criterion::default()
        // Configure defaults before overriding with args.
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1))
        .configure_from_args();
    mem::arena::bench_arena(&mut c);
    mem::skiplist::bench_skiplist(&mut c);
    c.final_summary();
}
