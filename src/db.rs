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

use crate::config::Config;
use crate::util::error::WickError;
use crate::util::slice::Slice;

pub struct DB {}

pub fn open_db(config: Config) -> DB {
    DB::new()
}

impl DB {
    pub fn new() -> DB {
        DB {}
    }
    pub fn write(&self, key: Slice, value: Slice) -> Result<(), WickError> {
        println!("[write] key: {:?}, value: {:?}", &key, &value);
        Ok(())
    }

    pub fn get(&self, key: Slice) -> Option<Slice> {
        println!("[get] key: {:?}", &key);
        None
    }
}
