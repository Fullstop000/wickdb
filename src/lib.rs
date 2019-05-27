// Copyright 2019 Fullstop000 <fullstop1005@gmail.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// TODO: remove below when the implementation is stable
#![allow(dead_code)]

#![allow(clippy::unreadable_literal)]
#![allow(clippy::new_ret_no_self)]
#![allow(clippy::type_complexity)]
// See https://github.com/rust-lang/rust-clippy/issues/1608
#![allow(clippy::redundant_closure)]

extern crate libc;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
extern crate crc;
extern crate crossbeam_channel;
extern crate crossbeam_utils;
extern crate rand;
extern crate snap;

#[macro_use]
mod util;
pub mod batch;
pub mod cache;
mod compaction;
pub mod db;
pub mod filter;
mod iterator;
mod mem;
pub mod options;
mod record;
mod snapshot;
mod sstable;
mod table_cache;
mod version;
pub mod storage;

pub use util::status;
