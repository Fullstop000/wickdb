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
// found in the LICENSE file. See the AUTHORS file for names of contributors.

use crate::storage::Storage;
use std::io::Result;
use std::fs::{File, Metadata, remove_file, rename, create_dir_all};

pub struct FileStorage {

}

impl Storage for FileStorage {
    fn create(name: &str) -> Result<File> {
        File::create(name)
    }

    fn open(name: &str) -> Result<File> {
        File::open(name)
    }

    fn remove(name: &str) -> Result<()> {
        remove_file(name)
    }

    fn rename(old: &str, new: &str) -> Result<()> {
        rename(old, new)
    }

    fn mkdir_all(dir: &str) -> Result<()> {
        create_dir_all(dir)
    }

    fn lock(name: &str) -> Result<()> {
        unimplemented!()
    }

    fn list(dir: &str) -> Result<Vec<&str>> {
        unimplemented!()
    }

    fn stat(name: &str) -> Result<Metadata> {
        unimplemented!()
    }
}

