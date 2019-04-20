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

pub mod filename;
pub mod format;

use crate::batch::WriteBatch;
use crate::options::{ReadOptions, WriteOptions};
use crate::util::slice::Slice;
use crate::util::status::Status;

/// A `DB` is a persistent ordered map from keys to values.
/// A `DB` is safe for concurrent access from multiple threads without
/// any external synchronization.
pub trait DB {
    /// `put` sets the value for the given key. It overwrites any previous value
    /// for that key; a DB is not a multi-map.
    fn put(
        &mut self,
        write_opt: Option<WriteOptions>,
        key: Slice,
        value: Slice,
    ) -> Result<(), Status>;

    /// `get` gets the value for the given key. It returns `Status::NotFound` if the DB
    /// does not contain the key.
    fn get(&self, read_opt: Option<ReadOptions>, key: Slice) -> Result<Slice, Status>;

    /// `delete` deletes the value for the given key. It returns `Status::NotFound` if
    /// the DB does not contain the key.
    fn delete(&mut self, write_opt: Option<WriteOptions>, key: Slice) -> Result<(), Status>;

    /// `apply` applies the operations contained in the `WriteBatch` to the DB atomically.
    fn apply(&mut self, write_opt: Option<WriteOptions>, batch: WriteBatch) -> Result<(), Status>;

    /// `close` closes the DB.
    fn close(&mut self) -> Result<(), Status>;
}
