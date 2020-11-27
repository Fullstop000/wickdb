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

use std::env::temp_dir;
use std::ops::Range;
use std::thread;
use wickdb::file::FileStorage;
use wickdb::{BytewiseComparator, LevelFilter, Options, ReadOptions, WickDB, WriteOptions, DB};

fn main() {
    let mut options = Options::<BytewiseComparator>::default();
    options.logger_level = LevelFilter::Debug;
    let dir = temp_dir().join("test_wickdb");
    let mut db = WickDB::open_db(options, &dir, FileStorage::default()).unwrap();
    let mut handles = vec![];
    let threads = 4;
    let num_per_thread = 25000;
    for i in 0..threads {
        let range = Range {
            start: i * num_per_thread,
            end: (i + 1) * num_per_thread,
        };
        let db = db.clone();
        let h = thread::spawn(move || {
            for n in range {
                let k = format!("key {}", n);
                let v = format!("value {}", n);
                db.put(WriteOptions::default(), k.as_bytes(), v.as_bytes())
                    .unwrap();
            }
        });
        handles.push(h);
    }
    for h in handles.drain(..) {
        h.join().unwrap();
    }
    for i in 0..threads {
        let range = Range {
            start: i * num_per_thread,
            end: (i + 1) * num_per_thread,
        };
        let db = db.clone();
        let h = thread::spawn(move || {
            for n in range {
                let k = format!("key {}", n);
                let v = db.get(ReadOptions::default(), k.as_bytes()).unwrap();
                assert!(v.is_some(), "key {} not found", k);
                assert_eq!(v.unwrap().as_slice(), format!("value {}", n).as_bytes());
            }
        });
        handles.push(h);
    }
    for h in handles {
        h.join().unwrap();
    }
    db.destroy().unwrap();
}
