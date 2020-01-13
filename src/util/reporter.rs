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

use crate::record::reader::Reporter;
use crate::{Error, Result};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone)]
pub struct LogReporter {
    inner: Rc<RefCell<LogReporterInner>>,
}

struct LogReporterInner {
    ok: bool,
    reason: String,
}

impl LogReporter {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(LogReporterInner {
                ok: true,
                reason: "".to_owned(),
            })),
        }
    }
    pub fn result(&self) -> Result<()> {
        let inner = self.inner.borrow();
        if inner.ok {
            Ok(())
        } else {
            Err(Error::Corruption(inner.reason.clone()))
        }
    }
}

impl Reporter for LogReporter {
    fn corruption(&mut self, _bytes: u64, reason: &str) {
        self.inner.borrow_mut().ok = false;
        self.inner.borrow_mut().reason = reason.to_owned();
    }
}
