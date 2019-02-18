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

#![feature(box_syntax)]
#[macro_use]
extern crate log;
extern crate byteorder;
extern crate libc;
extern crate rand;

#[macro_use]
pub mod util;
pub mod config;
pub mod db;
pub mod mem;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
