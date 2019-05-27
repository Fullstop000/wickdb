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

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::mem;
use std::rc::Rc;
use std::result;

#[derive(Debug, Clone)]
pub enum Status {
    NotFound,
    Corruption,
    NotSupported,
    InvalidArgument,
    CompressionError,
    IOError,

    Unexpected,
    Default, // used for default
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match *self {
            Status::NotFound => "NotFoundError",
            Status::Corruption => "CorruptionError",
            Status::NotSupported => "NotSupportedError",
            Status::InvalidArgument => "InvalidArgumentError",
            Status::CompressionError => "CompressionError",
            Status::IOError => "IOError",
            Status::Unexpected => "UnexpectedError",
            _ => "",
        }
    }
}

#[derive(Debug)]
pub struct WickErr {
    t: Status,
    msg: Option<&'static str>,
    raw: Option<Rc<Box<dyn Error>>>,
}

impl WickErr {
    pub fn new(t: Status, msg: Option<&'static str>) -> Self {
        Self { t, msg, raw: None }
    }

    pub fn new_from_raw(t: Status, msg: Option<&'static str>, raw: Box<dyn Error>) -> Self {
        Self {
            t,
            msg,
            raw: Some(Rc::new(raw)),
        }
    }

    #[inline]
    pub fn take_raw(&mut self) -> Option<Rc<Box<dyn Error>>> {
        mem::replace(&mut self.raw, None)
    }

    #[inline]
    pub fn status(&self) -> Status {
        self.t.clone()
    }
}

unsafe impl Send for WickErr {}
unsafe impl Sync for WickErr {}

pub type Result<T> = result::Result<T, WickErr>;

#[macro_export]
macro_rules! w_io_result {
    ($result:expr) => {
        match $result {
            Ok(v) => Ok(v),
            Err(e) => Err(WickErr::new_from_raw(Status::IOError, None, Box::new(e))),
        }
    };
}

impl Clone for WickErr {
    fn clone(&self) -> Self {
        Self {
            t: self.t.clone(),
            msg: self.msg,
            raw: self.raw.clone(),
        }
    }
}

impl Default for WickErr {
    fn default() -> Self {
        Self {
            t: Status::Default,
            msg: None,
            raw: None,
        }
    }
}

impl Display for WickErr {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        match self.msg {
            Some(m) => match &self.raw {
                Some(e) => {
                    return write!(
                        f,
                        "WickDB error [{}] : {} , raw : {}",
                        self.t.as_str(),
                        m,
                        e.description()
                    );
                }
                None => {
                    return write!(f, "WickDB error [{}] : {}", self.t.as_str(), m);
                }
            },
            None => match &self.raw {
                Some(e) => {
                    return write!(
                        f,
                        "WickDB error [{}] : {}",
                        self.t.as_str(),
                        e.description()
                    );
                }
                None => {
                    return write!(f, "WickDB error [{}]", self.t.as_str());
                }
            },
        }
    }
}

impl ::std::error::Error for WickErr {
    fn description(&self) -> &str {
        match self.msg {
            Some(m) => m,
            None => match &self.raw {
                Some(e) => e.description(),
                None => "",
            },
        }
    }
}
