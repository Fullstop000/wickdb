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

use crate::filter::FilterPolicy;
use crate::util::coding::{decode_fixed_64, put_fixed_64};
use crate::util::comparator::Comparator;
use crate::util::slice::Slice;
use crate::util::varint::VarintU32;
use std::cmp::Ordering;
use std::fmt::{Debug, Error, Formatter};
use std::rc::Rc;
use std::sync::Arc;

/// The max key sequence number. The value is 2^56 - 1 because the seq number
/// only takes 56 bits when is serialized to `InternalKey`
pub const MAX_KEY_SEQUENCE: u64 = (1u64 << 56) - 1;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValueType {
    /// A value indicates that the key is deleted
    Deletion = 0,
    /// A normal value
    Value = 1,

    /// Unknown type
    Unknown,
}

/// `FOR_SEEK` defines the `ValueType` that should be passed when
/// constructing a `ParsedInternalKey` for seeking to a particular
/// sequence number (since we sort sequence numbers in decreasing order
/// and the value type is embedded as the low 8 bits in the sequence
/// number in internal keys, we need to use the highest-numbered
/// ValueType, not the lowest).
pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::Value;

impl From<u64> for ValueType {
    fn from(v: u64) -> Self {
        match v {
            1 => ValueType::Value,
            0 => ValueType::Deletion,
            _ => ValueType::Unknown,
        }
    }
}

/// `ParsedInternalKey` represents a internal key used in wickdb.
/// A `ParsedInternalKey` can be encoded into a `InternalKey` by `encode()`.
pub struct ParsedInternalKey {
    /// The user's normal used key
    pub user_key: Slice,
    /// The sequence number of the Key
    pub seq: u64,
    /// The value type
    pub value_type: ValueType,
}

impl ParsedInternalKey {
    /// Try to extract a `ParsedInternalKey` from given bytes. This might be dangerous since
    /// a `Slice` never guarantees the underlying data lives as long as enough.
    /// Returns `None` if data length is less than 8 or getting an unknown value type.
    pub fn decode_from(internal_key: Slice) -> Option<Self> {
        let size = internal_key.size();
        if size < 8 {
            return None;
        }
        let num = decode_fixed_64(&internal_key.as_slice()[size - 8..]);
        let t = ValueType::from(num & 0xff);
        if t == ValueType::Unknown {
            return None;
        }
        let seq = num >> 8;
        Some(Self {
            user_key: Slice::from(&internal_key.as_slice()[..size - 8]),
            seq,
            value_type: t,
        })
    }

    pub fn new(key: Slice, seq: u64, v_type: ValueType) -> Self {
        ParsedInternalKey {
            user_key: key,
            seq,
            value_type: v_type,
        }
    }

    /// Returns a `InternalKey` encoded from the `ParsedInternalKey` using
    /// the format described in the below comment of `InternalKey`
    #[inline]
    pub fn encode(&self) -> InternalKey {
        InternalKey::new(&self.user_key, self.seq, self.value_type)
    }
}

impl Debug for ParsedInternalKey {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(
            f,
            "{:?} @ {} : {:?}",
            self.user_key, self.seq, self.value_type
        )
    }
}

/// A `InternalKey` is a encoding of a `ParsedInternalKey`
///
/// The format of `InternalKey`:
///
/// ```text
/// | ----------- n bytes ----------- | --- 7 bytes --- | - 1 byte - |
///              user key                  seq number        type
/// ```
///
#[derive(Clone, PartialEq, Eq)]
pub struct InternalKey {
    data: Vec<u8>,
}

impl InternalKey {
    pub fn new(key: &Slice, seq: u64, t: ValueType) -> Self {
        let mut v = Vec::from(key.as_slice());
        put_fixed_64(&mut v, pack_seq_and_type(seq, t));
        InternalKey { data: v }
    }

    #[inline]
    pub fn decoded_from(src: &[u8]) -> Self {
        // TODO: avoid copy here
        Self {
            data: Vec::from(src),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn user_key(&self) -> &[u8] {
        let length = self.data.len();
        &self.data.as_slice()[length - 8..]
    }

    /// Returns a `ParsedInternalKey`
    pub fn parsed(&self) -> Option<ParsedInternalKey> {
        let size = self.data.len();
        let user_key = Slice::from(&(self.data.as_slice())[0..size - 8]);
        let num = decode_fixed_64(&(self.data.as_slice())[size - 8..]);
        let t = ValueType::from(num & 0xff as u64);
        match t {
            ValueType::Unknown => None,
            _ => Some(ParsedInternalKey {
                user_key,
                seq: num >> 8,
                value_type: t,
            }),
        }
    }
}

impl Debug for InternalKey {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        if let Some(parsed) = self.parsed() {
            write!(f, "{:?}", parsed)
        } else {
            let s = unsafe { ::std::str::from_utf8_unchecked(self.data.as_slice()) };
            write!(f, "(bad){}", s)
        }
    }
}

impl Default for InternalKey {
    fn default() -> Self {
        InternalKey { data: vec![] }
    }
}

/// The format of a `LookupKey`:
///
/// ```text
///
///   +---------------------------------+
///   | varint32 of internal key length |
///   +---------------------------------+ --------------- user key start
///   | user key bytes                  |
///   +---------------------------------+   internal key
///   | sequence (7)        |  seek (1) |
///   +---------------------------------+ ---------------
///
/// ```
pub struct LookupKey {
    data: Vec<u8>,
    ukey_start: usize,
}

impl LookupKey {
    pub fn new(user_key: &[u8], seq_number: u64) -> Self {
        let mut data = vec![];
        let ukey_start = VarintU32::put_varint(&mut data, (user_key.len() + 8) as u32);
        data.extend_from_slice(user_key);
        put_fixed_64(
            &mut data,
            pack_seq_and_type(seq_number, VALUE_TYPE_FOR_SEEK),
        );
        Self { data, ukey_start }
    }

    /// Returns a key suitable for lookup in a MemTable.
    /// NOTICE: the LookupKey self should live at least as long as the returning Slice
    pub fn mem_key(&self) -> Slice {
        Slice::from(self.data.as_slice())
    }

    /// Returns an internal key (suitable for passing to an internal iterator)
    /// NOTICE: the LookupKey self should live at least as long as the returning Slice
    pub fn internal_key(&self) -> Slice {
        Slice::from(&self.data.as_slice()[self.ukey_start..])
    }

    /// Returns the user key
    /// NOTICE: the LookupKey self should live at least as long as the returning Slice
    pub fn user_key(&self) -> Slice {
        let len = self.data.len();
        Slice::from(&self.data.as_slice()[self.ukey_start..len - 8])
    }
}

/// `InternalKeyComparator` is used for comparing the `InternalKey`
/// the compare result is ordered by:
///    increasing user key (according to user-supplied comparator)
///    decreasing sequence number
///    decreasing type (though sequence# should be enough to disambiguate)
pub struct InternalKeyComparator {
    /// The comparator defined in `Options`
    pub user_comparator: Arc<dyn Comparator>,
}

impl InternalKeyComparator {
    pub fn new(ucmp: Arc<dyn Comparator>) -> Self {
        InternalKeyComparator {
            user_comparator: ucmp,
        }
    }
}

impl Comparator for InternalKeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let ua = extract_user_key(a);
        let ub = extract_user_key(b);
        // compare user key first
        match self.user_comparator.compare(ua.as_slice(), ub.as_slice()) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                let sa = extract_seq_number(a);
                let sb = extract_seq_number(b);
                // use the reverse order of the sequence number as result
                // since the key with a larger seq will be seek first
                if sa > sb {
                    Ordering::Less
                } else if sa == sb {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
        }
    }

    #[inline]
    fn name(&self) -> &str {
        "leveldb.InternalKeyComparator"
    }

    fn separator(&self, _a: &[u8], _b: &[u8]) -> Vec<u8> {
        unimplemented!()
    }

    fn successor(&self, _s: &[u8]) -> Vec<u8> {
        unimplemented!()
    }
}

/// A wrapper for the internal key filter policy
pub struct InternalFilterPolicy {
    user_policy: Rc<dyn FilterPolicy>,
}

impl FilterPolicy for InternalFilterPolicy {
    fn name(&self) -> &str {
        self.user_policy.name()
    }

    fn may_contain(&self, filter: &[u8], key: &Slice) -> bool {
        let user_key = extract_user_key(key.as_slice());
        self.user_policy.may_contain(filter, &user_key)
    }

    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
        let mut user_keys = vec![];
        for key in keys.iter() {
            let user_key = extract_user_key(key.as_slice());
            // TODO: avoid copying here
            user_keys.push(Vec::from(user_key.as_slice()))
        }
        self.user_policy.create_filter(user_keys.as_slice())
    }
}

// use a `Slice` to represent only the user key in a internal key slice
#[inline]
pub fn extract_user_key(key: &[u8]) -> Slice {
    let size = key.len();
    assert!(
        size >= 8,
        "[internal key] invalid size of internal key : expect >= 8 but got {}",
        size
    );
    Slice::new(key.as_ptr(), size - 8)
}

// get the sequence number from a internal key slice
#[inline]
fn extract_seq_number(key: &[u8]) -> u64 {
    let size = key.len();
    assert!(
        size >= 8,
        "[internal key] invalid size of internal key : expect >= 8 but got {}",
        size
    );
    decode_fixed_64(&key[size - 8..]) >> 8
}

#[inline]
// compose sequence number and value type into a single u64
fn pack_seq_and_type(seq: u64, v_type: ValueType) -> u64 {
    assert!(
        seq <= MAX_KEY_SEQUENCE,
        "[key seq] the sequence number should be <= {}, but got {}",
        MAX_KEY_SEQUENCE,
        seq
    );
    seq << 8 | v_type as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_seq_and_type() {
        let mut tests: Vec<(u64, ValueType, Vec<u8>)> = vec![
            (1, ValueType::Value, vec![1, 1, 0, 0, 0, 0, 0, 0]),
            (2, ValueType::Deletion, vec![0, 2, 0, 0, 0, 0, 0, 0]),
            (
                MAX_KEY_SEQUENCE,
                ValueType::Deletion,
                vec![0, 255, 255, 255, 255, 255, 255, 255],
            ),
        ];
        for (seq, t, expect) in tests.drain(..) {
            let u = decode_fixed_64(expect.as_slice());
            assert_eq!(pack_seq_and_type(seq, t), u);
        }
    }

    #[test]
    #[should_panic]
    fn test_pack_seq_and_type_panic() {
        pack_seq_and_type(1 << 56, ValueType::Value);
    }

    fn assert_encoded_decoded(key: &str, seq: u64, vt: ValueType) {
        let encoded = InternalKey::new(&Slice::from(key), seq, vt);
        let decoded = encoded.parsed().expect("");
        assert_eq!(key, decoded.user_key.as_str());
        assert_eq!(seq, decoded.seq);
        assert_eq!(vt, decoded.value_type);
    }

    #[test]
    fn test_internal_key_encode_decode() {
        let test_keys = ["", "k", "hello", "longggggggggggggggggggggg"];
        let test_seqs = [
            1,
            2,
            3,
            (1u64 << 8) - 1,
            1u64 << 8,
            (1u64 << 8) + 1,
            (1u64 << 16) - 1,
            1u64 << 16,
            (1u64 << 16) + 1,
            (1u64 << 32) - 1,
            1u64 << 32,
            (1u64 << 32) + 1,
        ];
        for i in 0..test_keys.len() {
            for j in 0..test_seqs.len() {
                assert_encoded_decoded(test_keys[i], test_seqs[j], ValueType::Value);
                assert_encoded_decoded(test_keys[i], test_seqs[j], ValueType::Deletion);
            }
        }
    }
}
