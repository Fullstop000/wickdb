## wickdb

[![Build Status](https://travis-ci.com/Fullstop000/wickdb.svg?branch=master)](https://travis-ci.org/Fullstop000/wickdb)
[![codecov](https://codecov.io/gh/Fullstop000/wickdb/branch/master/graph/badge.svg)](https://codecov.io/gh/Fullstop000/wickdb)
<a href="https://www.repostatus.org/#wip"><img src="https://www.repostatus.org/badges/latest/wip.svg" alt="Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public." /></a>
[![dependency status](https://deps.rs/repo/github/Fullstop000/wickdb/status.svg)](https://deps.rs/repo/github/Fullstop000/wickdb)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FFullstop000%2Fwickdb.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2FFullstop000%2Fwickdb?ref=badge_shield)

**This project is under rapidly development**

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
  - [x] Storage
  - [x] DB
- [x] Compaction implementation
- [x] Scheduling

#### [ongoing] Test cases & Benches

- Adding more test cases. The progress is tracked by this [issue](https://github.com/Fullstop000/wickdb/issues/3).
- Adding benchmarks. The progress is tracked by this [issue](https://github.com/Fullstop000/wickdb/issues/21).

### Developing

`wickdb` is built using the latest version of `stable` Rust, using [the 2018 edition](https://doc.rust-lang.org/edition-guide/rust-2018/).

In order to have your PR merged running the following must finish without error otherwise the CI will fail:

```bash
cargo test --all && \
cargo clippy && \
cargo fmt --all -- --check
```

You may optionally want to install `cargo-watch` to allow for automated rebuilding while editing:

```bash
cargo watch -s "cargo check --tests"
```

There're so many `TODO`s in current implementation and you can pick either of them to do something.

This crate is still at early stage so any PRs or issues are welcomed!.

## License

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FFullstop000%2Fwickdb.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2FFullstop000%2Fwickdb?ref=badge_large)
