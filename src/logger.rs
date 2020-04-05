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

use crate::db::filename::{generate_filename, FileType};
use crate::storage::{File, Storage};

use log::{LevelFilter, Log, Metadata, Record};
use slog::{o, Drain, Level};

use std::sync::Mutex;

/// A `slog` based logger which can be used with `log` crate
///
/// See `slog` at https://github.com/slog-rs/slog
/// See `log` at https://github.com/rust-lang/log
pub struct Logger {
    inner: slog::Logger,
    level: LevelFilter,
}

impl Logger {
    /// Create a logger backend
    ///
    /// If `inner` is not `None`, use `inner` logger
    /// If `inner` is `None`
    ///     - In dev mode, use a std output
    ///     - In release mode, use a storage specific file with name `LOG`
    pub fn new<S: Storage>(
        inner: Option<slog::Logger>,
        level: LevelFilter,
        storage: &S,
        db_path: &str,
    ) -> Self {
        let inner = match inner {
            Some(l) => l,
            None => {
                if cfg!(debug_assertions) {
                    // Use std out
                    let decorator = slog_term::TermDecorator::new().build();
                    let drain = Mutex::new(slog_term::FullFormat::new(decorator).build()).fuse();
                    slog::Logger::root(drain, o!())
                } else {
                    // Use a file `Log` to record all logs
                    // TODO: add file rotation
                    let file = storage
                        .create(generate_filename(db_path, FileType::InfoLog, 0).as_str())
                        .unwrap();
                    let drain = slog_async::Async::new(FileBasedDrain::new(file))
                        .build()
                        .fuse();
                    slog::Logger::root(drain, o!())
                }
            }
        };
        Self { inner, level }
    }
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    #[allow(unused_must_use)]
    fn log(&self, r: &Record) {
        if self.enabled(r.metadata()) {
            let level = log_to_slog_level(r.metadata().level());
            let args = r.args();
            let target = r.target();
            let module = r.module_path_static().unwrap_or("");
            let file = r.file_static().unwrap_or("");
            let line = r.line().unwrap_or(0);

            let s = slog::RecordStatic {
                location: &slog::RecordLocation {
                    file,
                    line,
                    column: 0,
                    function: "",
                    module,
                },
                level,
                tag: target,
            };
            if cfg!(debug_assertions) {
                let meta_info = format!("{}:{}", file, line);
                self.inner.log(&slog::Record::new(
                    &s,
                    args,
                    slog::b!("[location]" => meta_info),
                ))
            } else {
                self.inner.log(&slog::Record::new(&s, args, slog::b!()))
            }
        }
    }

    fn flush(&self) {}
}

fn log_to_slog_level(level: log::Level) -> Level {
    match level {
        log::Level::Trace => Level::Trace,
        log::Level::Debug => Level::Debug,
        log::Level::Info => Level::Info,
        log::Level::Warn => Level::Warning,
        log::Level::Error => Level::Error,
    }
}

struct FileBasedDrain<F: File> {
    inner: Mutex<F>,
}

impl<F: File> FileBasedDrain<F> {
    fn new(f: F) -> Self {
        FileBasedDrain {
            inner: Mutex::new(f),
        }
    }
}

impl<F: File> Drain for FileBasedDrain<F> {
    type Ok = ();
    type Err = slog::Never;

    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        // Ignore errors here
        let _ = self.inner.lock().unwrap().write(
            format!(
                "[{}] : {:?} \n {:?} \n",
                record.level(),
                record.msg(),
                values
            )
            .as_bytes(),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::mem::MemStorage;

    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_default_logger() {
        let s = MemStorage::default();
        let db_path = "test";
        let logger = Logger::new(None, LevelFilter::Debug, &s, db_path);
        // Ignore the error if the logger have been set
        let _ = log::set_logger(Box::leak(Box::new(logger)));
        log::set_max_level(LevelFilter::Debug);
        info!("Hello World");
        // Wait for the async logger print the result
        thread::sleep(Duration::from_millis(100));
    }
}
