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

use crate::db::format::InternalKey;
use hashbrown::HashSet;
use crate::util::varint::{VarintU32, VarintU64};
use crate::version::version_edit::Tag::{Comparator, LogNumber, PrevLogNumber, NextFileNumber, LastSequence, CompactPointer, DeletedFile, NewFile, Unknown};
use crate::util::status::{Result, WickErr, Status};
use crate::util::slice::Slice;
use std::fmt::{Debug, Formatter};
use std::mem;

// Tags for the VersionEdit disk format.
// Tag 8 is no longer used.
enum Tag {
    Comparator = 1,
    LogNumber = 2,
    NextFileNumber = 3,
    LastSequence = 4,
    CompactPointer = 5,
    DeletedFile = 6,
    NewFile = 7,
    // 8 was used for large value refs
    PrevLogNumber = 9,
    Unknown // unknown tag
}

impl From<u32> for Tag {
    fn from(i: u32) -> Self {
        match i {
            1 => Tag::Comparator,
            2 => Tag::LogNumber,
            3 => Tag::NextFileNumber,
            4 => Tag::LastSequence,
            5 => Tag::CompactPointer,
            6 => Tag::DeletedFile,
            7 => Tag::NewFile,
            9 => Tag::PrevLogNumber,
            _ => Tag::Unknown,
        }
    }
}

pub struct FileMetaData {
    // Seeks allowed until compaction
    allowed_seeks: usize,
    // File size in bytes
    file_size: u64,
    // the file number
    number: u64,
    // Smallest internal key served by table
    smallest: InternalKey,
    // Largest internal key served by table
    largest: InternalKey,
}


pub struct VersionEdit {
    max_levels: u8,

    // comparator name
    comparator_name: Option<String>,
    // file number of .log
    log_number: Option<u64>,
    prev_log_number: Option<u64>,
    next_file_number: Option<u64>,
    // the last used sequence number
    last_sequence: Option<u64>,

    // (level, InternalKey)
    compaction_pointers: Vec<(usize, InternalKey)>,
    // (level, file_number)
    deleted_files: HashSet<(usize, u64)>,
    // (level, FileMetaData)
    new_files: Vec<(usize, FileMetaData)>,
}

impl VersionEdit {
    pub fn new(max_levels: u8) -> Self {
        Self {
            max_levels,
            comparator_name: None,
            log_number: None,
            prev_log_number: None,
            next_file_number: None,
            last_sequence: None,
            deleted_files: HashSet::new(),
            new_files: Vec::new(),
            compaction_pointers: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.comparator_name = None;
        self.log_number = None;
        self.prev_log_number =  None;
        self.next_file_number = None;
        self.last_sequence = None;
        self.deleted_files.clear();
        self.new_files.clear();
        // compaction pointers not cleared here
    }

    /// Add the specified file at the specified number
    pub fn add_file(&mut self, level: usize, file_number: u64, file_size: u64, smallest: InternalKey, largest: InternalKey) {
        self.new_files.push((level, FileMetaData {
            allowed_seeks: 0,
            file_size,
            number: file_number,
            smallest,
            largest
        }))
    }

    /// Delete the specified file from the specified level
    pub fn delete_file(&mut self, level: usize, file_number: u64) {
        self.deleted_files.insert((level, file_number));
    }

    pub fn add_compaction_pointer(&mut self, level: usize, key: InternalKey) {
        self.compaction_pointers.push((level, key))
    }


    pub fn set_comparator_name(&mut self, name: String) {
        mem::replace::<Option<String>>(&mut self.comparator_name, Some(name));
    }

    pub fn set_log_number(&mut self, log_num: u64) {
        mem::replace::<Option<u64>>(&mut self.log_number, Some(log_num));
    }

    pub fn set_prev_log_number(&mut self, num: u64) {
        mem::replace::<Option<u64>>(&mut self.prev_log_number, Some(num));
    }

    pub fn set_next_file(&mut self, file_num: u64) {
        mem::replace::<Option<u64>>(&mut self.next_file_number, Some(file_num));
    }

    pub fn set_last_sequence(&mut self, seq: u64) {
        mem::replace::<Option<u64>>(&mut self.last_sequence, Some(seq));
    }

    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        if let Some(cmp_name) = &self.comparator_name {
            VarintU32::put_varint(dst, Comparator as u32);
            VarintU32::put_varint_prefixed_slice(dst, cmp_name.as_bytes());
        }
        if let Some(log_number) = &self.log_number {
            VarintU32::put_varint(dst, LogNumber as u32);
            VarintU64::put_varint(dst, *log_number);
        }
        if let Some(pre_ln) = &self.prev_log_number {
            VarintU32::put_varint(dst, PrevLogNumber as u32);
            VarintU64::put_varint(dst, *pre_ln);
        }
        if let Some(next_fn) = &self.next_file_number {
            VarintU32::put_varint(dst, NextFileNumber as u32);
            VarintU64::put_varint(dst, *next_fn);
        }

        if let Some(last_seq) = &self.last_sequence {
            VarintU32::put_varint(dst, LastSequence as u32);
            VarintU64::put_varint(dst, *last_seq);
        }

        for (level, key) in self.compaction_pointers.iter() {
            VarintU32::put_varint(dst, CompactPointer as u32);
            VarintU32::put_varint(dst, *level as u32);
            VarintU32::put_varint_prefixed_slice(dst, key.data());
        }

        for (level, file_num) in self.deleted_files.iter() {
            VarintU32::put_varint(dst, DeletedFile as u32);
            VarintU32::put_varint(dst, *level as u32);
            VarintU64::put_varint(dst, *file_num);
        }

        for (level, file_meta) in self.new_files.iter() {
            VarintU32::put_varint(dst, NewFile as u32);
            VarintU32::put_varint(dst, *level as u32);
            VarintU64::put_varint(dst, file_meta.number);
            VarintU64::put_varint(dst, file_meta.file_size);
            VarintU32::put_varint_prefixed_slice(dst, file_meta.smallest.data());
            VarintU32::put_varint_prefixed_slice(dst, file_meta.largest.data());
        }
    }

    pub fn decoded_from(&mut self, src: &[u8]) -> Result<()> {
        self.clear();
        let mut msg = String::new();
        let mut s = Slice::from(src);
        while !s.empty() {
            // decode tag
            if let Some(tag) = VarintU32::drain_read(&mut s) {
                match Tag::from(tag) {
                    Comparator => {
                        // decode comparator name
                        if let Some(cmp) = VarintU32::get_varint_prefixed_slice(&mut s){
                            self.comparator_name = Some(String::from(cmp.as_str()))
                        } else {
                            msg.push_str("comparator name");
                            break
                        }
                    }
                    LogNumber => {
                        // decode log number
                        if let Some(log_num) = VarintU64::drain_read(&mut s) {
                            self.log_number = Some(log_num);
                        } else {
                            msg.push_str("log number");
                            break
                        }
                    }
                    NextFileNumber => {
                        // decode next file number
                        if let Some(next_file_num) = VarintU64::drain_read(&mut s) {
                            self.next_file_number = Some(next_file_num);
                        } else {
                            msg.push_str("previous log number");
                            break
                        }
                    }
                    LastSequence => {
                        // decode last sequence
                        if let Some(last_seq) = VarintU64::drain_read(&mut s) {
                            self.last_sequence = Some(last_seq);
                        } else {
                            msg.push_str("last sequence number");
                            break
                        }
                    }
                    CompactPointer => {
                        // decode compact pointer
                        if let Some(level) = get_level(self.max_levels, &mut s) {
                            if let Some(key) = get_internal_key(&mut s) {
                                self.compaction_pointers.push((level as usize, key));
                                continue;
                            }
                        }
                        msg.push_str("compaction pointer");
                        break
                    }
                    DeletedFile => {
                        if let Some(level) = get_level(self.max_levels, &mut s) {
                            if let Some(file_num) = VarintU64::drain_read(&mut s) {
                                self.deleted_files.insert((level as usize, file_num));
                                continue;
                            }
                        }
                        msg.push_str("deleted file");
                        break
                    }
                    NewFile => {
                        if let Some(level) = get_level(self.max_levels, &mut s) {
                            if let Some(number) = VarintU64::drain_read(&mut s) {
                                if let Some(file_size) = VarintU64::drain_read(&mut s) {
                                    if let Some(smallest) = get_internal_key(&mut s) {
                                        if let Some(largest) = get_internal_key(&mut s) {
                                            self.new_files.push((level as usize, FileMetaData {
                                                allowed_seeks: 0,
                                                file_size,
                                                number,
                                                smallest,
                                                largest
                                            }));
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        msg.push_str("new-file entry");
                        break
                    }
                    PrevLogNumber => {
                        // decode pre log number
                        if let Some(pre_ln) = VarintU64::drain_read(&mut s) {
                            self.prev_log_number = Some(pre_ln);
                        } else {
                            msg.push_str("previous log number");
                            break
                        }
                    }
                    Unknown => {
                        msg.push_str("unknown tag");
                        break
                    }
                }
            } else if !src.is_empty() {
                msg.push_str("invalid tag");
            } else {
                break
            }
        }
        if !msg.is_empty() {
            let mut m = "VersionEdit: ".to_owned();
            m.push_str(msg.as_str());
            let s : &'static str= Box::leak(m.into_boxed_str());
            return Err(WickErr::new(Status::Corruption, Some(s)));
        }
        Ok(())
    }
}

impl Debug for VersionEdit {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "VersionEdit {{")?;
        if let Some(comparator) = &self.comparator_name {
            write!(f, "\n  Comparator: {}", comparator)?;
        }
        if let Some(log_number) = &self.log_number {
            write!(f, "\n  LogNumber: {}", log_number)?;
        }
        if let Some(prev_log_num) = &self.prev_log_number {
            write!(f, "\n  PrevLogNumber: {}", prev_log_num)?;
        }
        if let Some(next_file_num) = &self.next_file_number {
            write!(f, "\n  NextFile: {}", next_file_num)?;
        }
        if let Some(last_seq) = &self.last_sequence {
            write!(f, "\n  LastSeq: {}", last_seq)?;
        }
        for (level, key) in self.compaction_pointers.iter() {
            write!(f, "\n  CompactPointer: {} {:?}", level, key)?;
        }
        for (level, file_num) in self.deleted_files.iter() {
            write!(f, "\n  DeleteFile: {} {}", level, file_num)?;
        }
        for (level,  meta) in self.new_files.iter() {
            write!(
                f,
                "\n  AddFile: {} {} {} {:?}..{:?}",
                level, meta.number, meta.file_size, meta.smallest, meta.largest
            )?;
        }
        write!(f, "\n}}\n")?;
        Ok(())
    }
}

fn get_internal_key(mut src: &mut Slice) -> Option<InternalKey> {
    if let Some(s) = VarintU32::get_varint_prefixed_slice(&mut src) {
        return Some(InternalKey::decoded_from(s.to_slice()));
    }
    None
}

fn get_level(max_levels: u8, src: &mut Slice) -> Option<u32> {
    match VarintU32::drain_read(src) {
        Some(l) => {
            if l<= max_levels as u32 {
                return Some(l)
            }
            None
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::version::version_edit::VersionEdit;
    use crate::db::format::{InternalKey, ValueType};
    use crate::util::slice::Slice;

    fn assert_encode_decode(edit: &VersionEdit) {
        let mut encoded = vec![];
        edit.encode_to(&mut encoded);
        let mut parsed = VersionEdit::new(7);
        parsed.decoded_from(encoded.as_slice()).expect("");
        let mut encoded2 = vec![];
        parsed.encode_to(&mut encoded2);
        assert_eq!(encoded, encoded2)
    }

    #[test]
    fn test_encode_decode() {
        let k_big = 1u64<<50;
        let mut edit = VersionEdit::new(7);
        for i in 0..4 {
            assert_encode_decode(&edit);
            edit.add_file(3, k_big + 300 + i, k_big + 400 + i,
                InternalKey::new(&Slice::from("foo"), k_big + 500 + i, ValueType::Value),
                InternalKey::new(&Slice::from("zoo"), k_big + 700 + i, ValueType::Deletion));
            edit.delete_file(4, k_big + 700 + i);
            edit.add_compaction_pointer(i as usize, InternalKey::new(&Slice::from("x"), k_big + 900 + i, ValueType::Value));
        }
        edit.set_comparator_name("foo".to_owned());
        edit.set_log_number(k_big + 100);
        edit.set_next_file(k_big + 200);
        edit.set_last_sequence(k_big + 1000);
        assert_encode_decode(&edit);
    }
}