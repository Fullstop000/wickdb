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

#![allow(dead_code)]
#![allow(unused_variables)]

#[macro_use]
extern crate log;
extern crate libc;
#[macro_use]
extern crate lazy_static;
extern crate rand;
extern crate crc;
extern crate snap;

#[macro_use]
mod util;
pub mod cache;
pub mod batch;
pub mod db;
mod iterator;
pub mod options;
pub mod filter;
mod mem;
mod table;
mod snapshot;
mod record;
pub mod storage;
