// Copyright 2020 Fullstop000 <fullstop1005@gmail.com>.
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

use crossbeam_channel::RecvError;
use quick_error::quick_error;
quick_error! {
    #[derive(Debug)]
    pub enum Error {
        /// If the hint is `None`, the key is deleted
        NotFound(hint: Option<String>) {
            display("key seeking failed: {:?}", hint)
        }
        Corruption(hint: String) {
            display("data corruption: {}", hint)
        }
        UTF8Error(err: std::string::FromUtf8Error) {
            display("UTF8 error: {:?}", err)
        }
        InvalidArgument(hint: String) {
            display("invalid argument: {}", hint)
        }
        DBClosed(hint: String) {
            display("try to operate a closed db: {}", hint)
        }
        CompressionFailed(err: snap::Error) {
            display("compression failed: {}", err)
            cause(err)
        }
        IO(err: std::io::Error) {
            display("I/O operation error: {}", err)
            cause(err)
        }
        RecvError(err: RecvError) {
            display("{:?}", err)
            cause(err)
        }
        Customized(hint: String) {
            display("{}", hint)
        }
    }
}

macro_rules! map_io_res {
    ($result:expr) => {
        match $result {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::IO(e)),
        }
    };
}

pub type Result<T> = std::result::Result<T, Error>;
