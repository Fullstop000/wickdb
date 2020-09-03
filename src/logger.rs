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
use crate::error::Result;
use crate::storage::{File, Storage};

use log::{LevelFilter, Log, Metadata, Record};
use slog::{o, Drain, Level};

use chrono::prelude::*;
use std::sync::Mutex;
/// A `slog` based logger which can be used with `log` crate
///
/// See `slog` at https://github.com/slog-rs/slog
/// See `log` at https://github.com/rust-lang/log
///

fn create_file<S: Storage>(storage: &S, dp_path: &str, timestamp: i64) -> Result<S::F> {
    let new_path = generate_filename(dp_path, FileType::OldInfoLog, timestamp as u64);
    storage.rename(dp_path, new_path.as_str())?;
    storage.create(dp_path)
}

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
    pub fn new<S: Storage + Clone + 'static>(
        inner: Option<slog::Logger>,
        level: LevelFilter,
        storage: S,
        db_path: String,
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
                    let file =
                        create_file(&storage, db_path.as_str(), Local::now().timestamp()).unwrap();
                    let file_fn = move |path: String| {
                        create_file(&storage, path.as_str(), Local::now().timestamp())
                    };
                    let drain = FileBasedDrain::new(file, db_path.clone(), file_fn)
                        .add_rotator(RotatedFileBySize::new(0));
                    let drain = slog_async::Async::new(drain).build().fuse();
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
    rotators: Vec<Box<dyn Rotator>>,
    dp_path: String,
    new_file: Box<dyn Send + Fn(String) -> Result<F>>,
}

impl<F: File> FileBasedDrain<F> {
    fn new<H>(f: F, path: String, new_file: H) -> Self
    where
        H: 'static + Send + Fn(String) -> Result<F>,
    {
        FileBasedDrain {
            dp_path: path.clone(),
            inner: Mutex::new(f),
            rotators: vec![],
            new_file: Box::new(new_file),
        }
    }

    fn add_rotator<R: 'static + Rotator>(mut self, rotator: R) -> Self {
        if rotator.is_enabled() {
            self.rotators.push(Box::new(rotator));
        }
        for rotator in (&self).rotators.iter() {
            rotator.prepare(&*self.inner.lock().unwrap()).unwrap();
        }
        self
    }

    fn flush(&self) -> Result<()> {
        self.inner.lock().unwrap().flush()?;
        let new_file = (self.new_file)(self.dp_path.clone()).unwrap();

        let mut old_file = self.inner.lock().unwrap();
        std::mem::replace(&mut *old_file, new_file);
        for rotator in self.rotators.iter() {
            rotator.on_rotate()?;
        }
        return Ok(());
    }
}

impl<F: File> Drain for FileBasedDrain<F> {
    type Ok = ();
    type Err = slog::Never;

    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> std::result::Result<Self::Ok, Self::Err> {
        let by = format!(
            "[{}] : {:?} \n {:?} \n",
            record.level(),
            record.msg(),
            values
        );
        for rotator in self.rotators.iter() {
            if rotator.should_rotate() {
                self.flush().unwrap();
                return Ok(());
            }
        }

        for rotator in self.rotators.iter() {
            rotator.on_write(by.as_bytes()).unwrap();
        }
        // Ignore errors here
        let _ = self.inner.lock().unwrap().write(by.as_bytes());

        Ok(())
    }
}

trait Rotator: Send {
    /// Check if the option is enabled in configuration.
    /// Return true if the `rotator` is valid.
    fn is_enabled(&self) -> bool;

    /// Call by operator, initializes the states of rotators.
    fn prepare(&self, file: &dyn File) -> Result<()>;

    /// Return if the file need to be rotated.
    fn should_rotate(&self) -> bool;

    fn on_write(&self, buf: &[u8]) -> Result<()>;
    // Call by operator, update rotators' state while the operator execute a rotation.
    fn on_rotate(&self) -> Result<()>;
}

struct RotatedFileBySize {
    rotation_size: u64,
    file_size: Mutex<u64>,
}

impl RotatedFileBySize {
    fn new(rotation_size: u64) -> Self {
        RotatedFileBySize {
            rotation_size,
            file_size: Mutex::new(0),
        }
    }
}

impl Rotator for RotatedFileBySize {
    fn is_enabled(&self) -> bool {
        self.rotation_size != 0
    }
    fn prepare(&self, file: &dyn File) -> Result<()> {
        *self.file_size.lock().unwrap() = file.len().unwrap();
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        *self.file_size.lock().unwrap() > self.rotation_size
    }
    fn on_write(&self, buf: &[u8]) -> Result<()> {
        *self.file_size.lock().unwrap() += buf.len() as u64;
        Ok(())
    }

    fn on_rotate(&self) -> Result<()> {
        *self.file_size.lock().unwrap() = 0;
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
        // let s = &'static s;
        let db_path = "test";
        let logger = Logger::new(None, LevelFilter::Debug, s, db_path.to_string());
        // Ignore the error if the logger have been set
        let _ = log::set_logger(Box::leak(Box::new(logger)));
        log::set_max_level(LevelFilter::Debug);
        info!("Hello World");
        // Wait for the async logger print the result
        thread::sleep(Duration::from_millis(100));
    }
}
