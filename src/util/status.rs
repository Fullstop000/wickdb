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

use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Status {
    NotFound,
    Corruption,
    NotSupported,
    InvalidArgument,
    IOError
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match *self {
            Status::NotFound => "NotFoundError",
            Status::Corruption => "CorruptionError",
            Status::NotSupported => "NotSupportedError",
            Status::InvalidArgument => "InvalidArgumentError",
            Status::IOError => "IOError",
        }
    }
}

#[derive(Debug)]
pub struct WickErr {
    t: Status,
    msg: Option<&'static str>,
}

impl WickErr {
    pub fn new(t: Status, msg: Option<&'static str>) -> Self {
        Self {
            t,
            msg,
        }
    }
}

impl Display for WickErr {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        match self.msg {
            Some(m) => write!(f, "WickDB error [{}] : {}", self.t.as_str(), m),
            None => write!(f, "WickDB error [{}]", self.t.as_str()),
        }
    }
}

impl ::std::error::Error for WickErr {
    fn description(&self) -> &str {
        match self.msg {
            Some(m) => m,
            None => "",
        }
    }
}
