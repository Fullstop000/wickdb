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

use crate::storage::File;
use log::{LevelFilter, Log, Metadata, Record};
use std::sync::Mutex;

/// A simple file based Logger
// TODO: maybe use slog-rs instead
pub struct Logger<F: File> {
    file: Mutex<F>,
    level: LevelFilter,
}

// unsafe impl Send for Logger {}
// unsafe impl Sync for Logger {}

impl<F: File> Logger<F> {
    pub fn new(file: F, level: LevelFilter) -> Self {
        Self {
            file: Mutex::new(file),
            level,
        }
    }
}

impl<F: File> Log for Logger<F> {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    #[allow(unused_must_use)]
    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            self.file
                .lock()
                .unwrap()
                .write(format!("[{}] : {} \n", record.level(), record.args()).as_bytes());
        }
    }

    #[allow(unused_must_use)]
    fn flush(&self) {
        self.file.lock().unwrap().flush();
    }
}
