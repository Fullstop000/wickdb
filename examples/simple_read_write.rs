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
use wickdb::Options;
use wickdb::ReadOptions;
use wickdb::WickDB;
use wickdb::WriteOptions;
use wickdb::DB;

fn main() {
    let options = Options::default();
    let storage = FileStorage::default();
    let db = WickDB::open_db(options, "./test_db", storage).expect("could not open db");
    db.put(
        WriteOptions::default(),
        "key1".as_bytes(),
        "value1".as_bytes(),
    )
    .expect("could not success putting");
    db.put(
        WriteOptions::default(),
        "key2".as_bytes(),
        "value2".as_bytes(),
    )
    .expect("could not success putting");
    let val1 = db
        .get(ReadOptions::default(), "key1".as_bytes())
        .expect("could not get key1");
    let val2 = db
        .get(ReadOptions::default(), "key2".as_bytes())
        .expect("could not get key2");
    assert!(val1.is_some());
    assert!(val2.is_some());
    assert_eq!(val1.unwrap().as_str(), "value1");
    assert_eq!(val2.unwrap().as_str(), "value2");
}
