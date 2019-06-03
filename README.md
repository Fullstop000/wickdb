## wickdb

[![Build Status](https://travis-ci.org/Fullstop000/wickdb.svg?branch=master)](https://travis-ci.org/Fullstop000/wickdb)
[![Coverage Status](https://coveralls.io/repos/github/Fullstop000/wickdb/badge.svg?branch=master)](https://coveralls.io/github/Fullstop000/wickdb?branch=master)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2FFullstop000%2Fwickdb.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2FFullstop000%2Fwickdb?ref=badge_shield)

You can find a simple worked example in `examples`. 

### Plan & Progress

#### The basic shape of LevelDB

- [x] Fundamental components
  - [x] Arena
  - [x] Skiplist
  - [x] Cache
  - [x] Record
  - [x] Batch
  - [x] Block
  - [x] Table
  - [x] Version
  - [x] VersionEdit
  - [x] VersionSet
  - [x] Storage (aka Env)
  - [x] DB
- [x] Compaction implementation
- [x] Scheduling

#### [ongoing] Test cases & Benches

- [ ] Solid test cases
- [ ] Benchmark

#### Remove unsafe codes

We should use Rust as safe as we could because that's why we prefer using Rust.

- `Slice` could be replaced using crate [bytes](https://docs.rs/bytes).
- `LRUCache` is a double-linked circle list implemented by raw pointer.

### Development

For now we should focus on increasing test case coverage and benchmarks because they're so much significant to a storage engine.

Besides, there're so many `TODO`s in current implementation and you can pick either of them to do something.

Still, any pr or issue is welcomed.

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2FFullstop000%2Fwickdb.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2FFullstop000%2Fwickdb?ref=badge_large)