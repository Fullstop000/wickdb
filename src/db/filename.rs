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

use crate::storage::{do_write_string_to_file, Storage};
use crate::Result;
use std::ffi::OsStr;
use std::path::Path;

#[derive(Debug, PartialEq, Eq)]
pub enum FileType {
    /// `*.log` files guarantee crash consistency for DB.
    Log,
    /// `LOCK` file. Only one `DB` instance may acquire the file lock.
    Lock,
    /// `*.sst` file.
    Table,
    /// `MANIFEST-*` file.
    Manifest,
    /// `CURRENT` file saves the current used manifest filename.
    Current,
    /// `*.dbtmp` file
    Temp,
    /// `LOG` file records runtime logs. If there is a `LOG` file exists when the db starts,
    /// the old `LOG` file will be renamed to `LOG.old` and a new `LOG` file will be created.
    InfoLog,
    /// `LOG.old` file records the last runtime logs.
    OldInfoLog,
}

/// Returns a filename for a certain `FileType` by given sequence number and a `dirname`.
///
/// # Safety
/// `dirname` must be a valid unicode string  
pub fn generate_filename(dirname: &str, filetype: FileType, seq: u64) -> String {
    let dirname = Path::new(dirname).to_owned();
    match filetype {
        FileType::Log => dirname
            .join(format!("{:06}.log", seq))
            .into_os_string()
            .into_string()
            .unwrap(),
        FileType::Lock => dirname.join("LOCK").into_os_string().into_string().unwrap(),
        FileType::Table => dirname
            .join(format!("{:06}.sst", seq))
            .into_os_string()
            .into_string()
            .unwrap(),
        FileType::Manifest => dirname
            .join(format!("MANIFEST-{:06}", seq))
            .into_os_string()
            .into_string()
            .unwrap(),
        FileType::Current => dirname
            .join("CURRENT")
            .into_os_string()
            .into_string()
            .unwrap(),
        FileType::Temp => dirname
            .join(format!("{:06}.dbtmp", seq))
            .into_os_string()
            .into_string()
            .unwrap(),
        FileType::InfoLog => dirname.join("LOG").into_os_string().into_string().unwrap(),
        FileType::OldInfoLog => dirname
            .join("LOG.old")
            .into_os_string()
            .into_string()
            .unwrap(),
    }
}

/// Returns a tuple that contains `FileType` and the sequence number of the file.
/// The `filename` should be a valid path.
pub fn parse_filename<P: AsRef<Path>>(filename: P) -> Option<(FileType, u64)> {
    let invalid = "invalid";
    let path = filename.as_ref();
    let file_stem = path.file_stem().unwrap_or_else(|| OsStr::new(invalid));
    match file_stem.to_str() {
        Some("CURRENT") => Some((FileType::Current, 0)),
        Some("LOCK") => Some((FileType::Lock, 0)),
        Some("LOG") => match path.file_name().unwrap_or_else(|| OsStr::new("")).to_str() {
            Some("LOG") => Some((FileType::InfoLog, 0)),
            Some("LOG.old") => Some((FileType::OldInfoLog, 0)),
            _ => None,
        },
        Some(with_seq) => {
            if with_seq.starts_with("MANIFEST") {
                let strs: Vec<&str> = with_seq.split('-').collect();
                if strs.len() != 2 {
                    return None;
                }
                if let Ok(seq) = strs[1].parse::<u64>() {
                    return Some((FileType::Manifest, seq));
                }
                return None;
            };
            if let Ok(seq) = with_seq.parse::<u64>() {
                match path
                    .extension()
                    .unwrap_or_else(|| OsStr::new(invalid))
                    .to_str()
                {
                    Some("log") => {
                        return Some((FileType::Log, seq));
                    }
                    Some("sst") => {
                        return Some((FileType::Table, seq));
                    }
                    Some("dbtmp") => {
                        return Some((FileType::Temp, seq));
                    }
                    _ => {
                        return None;
                    }
                }
            };
            None
        }
        _ => None,
    }
}

/// Update the CURRENT file to point to new MANIFEST file
pub fn update_current<S: Storage>(env: &S, dir: &str, manifest_file_num: u64) -> Result<()> {
    let mut manifest = generate_filename(dir, FileType::Manifest, manifest_file_num);
    manifest.drain(0..=dir.len());
    let tmp = generate_filename(dir, FileType::Temp, manifest_file_num);
    let result = do_write_string_to_file(env, manifest, &tmp, true);
    match &result {
        Ok(()) => env.rename(&tmp, &generate_filename(dir, FileType::Current, 0))?,
        Err(_) => env.remove(&tmp)?,
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_filename() {
        let dirname = "test";
        let mut tests = if cfg!(windows) {
            vec![
                (FileType::Log, 10, "test\\000010.log"),
                (FileType::Lock, 1, "test\\LOCK"),
                (FileType::Table, 123, "test\\000123.sst"),
                (FileType::Manifest, 9, "test\\MANIFEST-000009"),
                (FileType::Current, 1, "test\\CURRENT"),
                (FileType::Temp, 100, "test\\000100.dbtmp"),
                (FileType::InfoLog, 1, "test\\LOG"),
                (FileType::OldInfoLog, 1, "test\\LOG.old"),
            ]
        } else {
            vec![
                (FileType::Log, 10, "test/000010.log"),
                (FileType::Lock, 1, "test/LOCK"),
                (FileType::Table, 123, "test/000123.sst"),
                (FileType::Manifest, 9, "test/MANIFEST-000009"),
                (FileType::Current, 1, "test/CURRENT"),
                (FileType::Temp, 100, "test/000100.dbtmp"),
                (FileType::InfoLog, 1, "test/LOG"),
                (FileType::OldInfoLog, 1, "test/LOG.old"),
            ]
        };

        for (ft, seq, expect) in tests.drain(..) {
            let name = generate_filename(dirname, ft, seq);
            assert_eq!(name, expect.to_owned());
        }
    }

    #[test]
    fn test_parse_filename() {
        let mut tests = if cfg!(windows) {
            vec![
                ("a\\b\\c\\000123.log", Some((FileType::Log, 123))),
                ("a\\b\\c\\LOCK", Some((FileType::Lock, 0))),
                ("a\\b\\c\\010666.sst", Some((FileType::Table, 10666))),
                ("a\\b\\c\\MANIFEST-000009", Some((FileType::Manifest, 9))),
                ("a\\b\\c\\000123.dbtmp", Some((FileType::Temp, 123))),
                ("a\\b\\c\\CURRENT", Some((FileType::Current, 0))),
                ("a\\b\\c\\LOG", Some((FileType::InfoLog, 0))),
                ("a\\b\\c\\LOG.old", Some((FileType::OldInfoLog, 0))),
                ("a\\b\\c\\test.123", None),
                ("a\\b\\c\\LOG.", None),
                ("a\\b\\c\\LOG.new", None),
                ("a\\b\\c\\000def.log", None),
                ("a\\b\\c\\MANIFEST-abcedf", None),
                ("a\\b\\c\\MANIFEST", None),
                ("a\\b\\c\\MANIFEST-123123-abcdef", None),
            ]
        } else {
            vec![
                ("a/b/c/000123.log", Some((FileType::Log, 123))),
                ("a/b/c/LOCK", Some((FileType::Lock, 0))),
                ("a/b/c/010666.sst", Some((FileType::Table, 10666))),
                ("a/b/c/MANIFEST-000009", Some((FileType::Manifest, 9))),
                ("a/b/c/000123.dbtmp", Some((FileType::Temp, 123))),
                ("a/b/c/CURRENT", Some((FileType::Current, 0))),
                ("a/b/c/LOG", Some((FileType::InfoLog, 0))),
                ("a/b/c/LOG.old", Some((FileType::OldInfoLog, 0))),
                // invalid conditions
                ("a/b/c/test.123", None),
                ("a/b/c/LOG.", None),
                ("a/b/c/LOG.new", None),
                ("a/b/c/000def.log", None),
                ("a/b/c/MANIFEST-abcedf", None),
                ("a/b/c/MANIFEST", None),
                ("a/b/c/MANIFEST-123123-abcdef", None),
            ]
        };

        for (filename, expect) in tests.drain(..) {
            let result = parse_filename(filename);
            assert_eq!(result, expect);
        }
    }
}
