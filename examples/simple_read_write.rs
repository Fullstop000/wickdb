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

use wickdb::file::FileStorage;
use wickdb::{BytewiseComparator, Options, ReadOptions, WickDB, WriteOptions, DB};

fn main() {
    let options = Options::<BytewiseComparator>::default();
    let storage = FileStorage::default();
    let db = WickDB::open_db(options, "./test_db", storage).unwrap();
    db.put(WriteOptions::default(), b"key1", b"value1").unwrap();
    db.put(WriteOptions::default(), b"key2", b"value2").unwrap();
    let val1 = db.get(ReadOptions::default(), b"key1").unwrap();
    let val2 = db.get(ReadOptions::default(), b"key2").unwrap();
    assert!(val1.is_some());
    assert!(val2.is_some());
    assert_eq!(val1.unwrap(), b"value1");
    assert_eq!(val2.unwrap(), b"value2");
}
