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

use std::mem::transmute;
use std::ptr::copy_nonoverlapping;

/// Encodes `value` in little-endian and puts it in the first 4-bytes of `dst`.
///
/// # Panics
///
/// Panics if `dst.len()` is less than 4.
pub fn encode_fixed_32(dst: &mut [u8], value: u32) {
    assert!(
        dst.len() >= 4,
        "the length of 'dst' must be at least 4 for a u32, but got {}",
        dst.len()
    );
    unsafe {
        let bytes = transmute::<u32, [u8; 4]>(value.to_le());
        copy_nonoverlapping(bytes.as_ptr(), dst.as_mut_ptr(), 4);
    }
}

/// Encodes `value` in little-endian and puts in the first 8-bytes of `dst`.
///
/// # Panics
///
/// Panics if `dst.len()` is less than 8.
pub fn encode_fixed_64(dst: &mut [u8], value: u64) {
    assert!(
        dst.len() >= 8,
        "the length of 'dst' must be at least 8 for a u64, but got {}",
        dst.len()
    );
    unsafe {
        let bytes = transmute::<u64, [u8; 8]>(value.to_le());
        copy_nonoverlapping(bytes.as_ptr(), dst.as_mut_ptr(), 8);
    }
}

/// Decodes the first 4-bytes of `src` in little-endian and returns the decoded value.
///
/// If the length of the given `src` is larger than 4, only use `src[0..4]`
pub fn decode_fixed_32(src: &[u8]) -> u32 {
    let mut data: u32 = 0;
    if src.len() >= 4 {
        unsafe {
            copy_nonoverlapping(src.as_ptr(), &mut data as *mut u32 as *mut u8, 4);
        }
    } else {
        for (i, b) in src.iter().enumerate() {
            data += (u32::from(*b)) << (i * 8);
        }
    }
    data.to_le()
}

/// Decodes the first 8-bytes of `src` in little-endian and returns the decoded value.
///
/// If the length of the given `src` is larger than 8, only use `src[0..8]`
pub fn decode_fixed_64(src: &[u8]) -> u64 {
    let mut data: u64 = 0;
    if src.len() >= 8 {
        unsafe {
            copy_nonoverlapping(src.as_ptr(), &mut data as *mut u64 as *mut u8, 8);
        }
    } else {
        for (i, b) in src.iter().enumerate() {
            data += (u64::from(*b)) << (i * 8);
        }
    }
    data.to_le()
}

/// Encodes the given u32 to bytes and concatenates the result to `dst`
pub fn put_fixed_32(dst: &mut Vec<u8>, value: u32) {
    let mut buf = [0u8; 4];
    encode_fixed_32(&mut buf[..], value);
    dst.extend_from_slice(&buf);
}

/// Encodes the given u64 to bytes and concatenates the result to `dst`
pub fn put_fixed_64(dst: &mut Vec<u8>, value: u64) {
    let mut buf = [0u8; 8];
    encode_fixed_64(&mut buf[..], value);
    dst.extend_from_slice(&buf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_fixed_32() {
        let mut tests: Vec<(u32, Vec<u8>, Vec<u8>)> = vec![
            (0u32, vec![0; 4], vec![0, 0, 0, 0]),
            (1u32, vec![0; 4], vec![1, 0, 0, 0]),
            (255u32, vec![0; 4], vec![255, 0, 0, 0]),
            (256u32, vec![0; 4], vec![0, 1, 0, 0]),
            (512u32, vec![0; 4], vec![0, 2, 0, 0]),
            (u32::max_value(), vec![0; 4], vec![255, 255, 255, 255]),
            (u32::max_value(), vec![0; 6], vec![255, 255, 255, 255, 0, 0]),
        ];
        for (input, mut dst, expect) in tests.drain(..) {
            encode_fixed_32(dst.as_mut_slice(), input);
            for (n, m) in dst.iter().zip(expect) {
                assert_eq!(*n, m);
            }
        }
    }

    #[test]
    fn test_decode_fixed_32() {
        let mut tests: Vec<(Vec<u8>, u32)> = vec![
            (vec![], 0u32),
            (vec![0], 0u32),
            (vec![1, 0], 1u32),
            (vec![1, 1], 257u32),
            (vec![0, 0, 0, 0], 0u32),
            (vec![1, 0, 0, 0], 1u32),
            (vec![255, 0, 0, 0], 255u32),
            (vec![0, 1, 0, 0], 256u32),
            (vec![0, 1], 256u32),
            (vec![0, 2, 0, 0], 512u32),
            (vec![255, 255, 255, 255], u32::max_value()),
            (vec![255, 255, 255, 255, 0, 0], u32::max_value()),
            (vec![255, 255, 255, 255, 1, 0], u32::max_value()),
        ];
        for (src, expect) in tests.drain(..) {
            let result = decode_fixed_32(src.as_slice());
            assert_eq!(result, expect);
        }
    }

    #[test]
    fn test_encode_fixed_64() {
        let mut tests: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (0u64, vec![0; 8], vec![0; 8]),
            (1u64, vec![0; 8], vec![1, 0, 0, 0, 0, 0, 0, 0]),
            (255u64, vec![0; 8], vec![255, 0, 0, 0, 0, 0, 0, 0]),
            (256u64, vec![0; 8], vec![0, 1, 0, 0, 0, 0, 0, 0]),
            (512u64, vec![0; 8], vec![0, 2, 0, 0, 0, 0, 0, 0]),
            (
                u64::max_value(),
                vec![0; 8],
                vec![255, 255, 255, 255, 255, 255, 255, 255],
            ),
            (
                u64::max_value(),
                vec![0; 10],
                vec![255, 255, 255, 255, 255, 255, 255, 255, 0, 0],
            ),
        ];
        for (input, mut dst, expect) in tests.drain(..) {
            encode_fixed_64(dst.as_mut_slice(), input);
            for (n, m) in dst.iter().zip(expect) {
                assert_eq!(*n, m);
            }
        }
    }

    #[test]
    fn test_decode_fixed_64() {
        let mut tests: Vec<(Vec<u8>, u64)> = vec![
            (vec![], 0u64),
            (vec![0], 0u64),
            (vec![0; 8], 0u64),
            (vec![1, 0], 1u64),
            (vec![1, 1], 257u64),
            (vec![1, 0, 0, 0, 0, 0, 0, 0], 1u64),
            (vec![255, 0, 0, 0, 0, 0, 0, 0], 255u64),
            (vec![0, 1, 0, 0, 0, 0, 0, 0], 256u64),
            (vec![0, 1], 256u64),
            (vec![0, 2, 0, 0, 0, 0, 0, 0], 512u64),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255],
                u64::max_value(),
            ),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255, 0, 0],
                u64::max_value(),
            ),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255, 1, 0],
                u64::max_value(),
            ),
        ];
        for (src, expect) in tests.drain(..) {
            let result = decode_fixed_64(src.as_slice());
            assert_eq!(result, expect);
        }
    }

    #[test]
    fn test_put_fixed32() {
        let mut s: Vec<u8> = vec![];
        for i in 0..100000u32 {
            put_fixed_32(&mut s, i);
        }
        for i in 0..100000u32 {
            let res = decode_fixed_32(s.as_mut_slice());
            assert_eq!(i, res);
            s.drain(0..4);
        }
    }

    #[test]
    fn test_put_fixed64() {
        let mut s: Vec<u8> = vec![];
        for power in 0..=63u64 {
            let v = 1 << power;
            put_fixed_64(&mut s, v - 1);
            put_fixed_64(&mut s, v);
            put_fixed_64(&mut s, v + 1);
        }
        for power in 0..=63u64 {
            let v = 1 << power;
            assert_eq!(v - 1, decode_fixed_64(s.as_mut_slice()));
            s.drain(0..8);
            assert_eq!(v, decode_fixed_64(s.as_mut_slice()));
            s.drain(0..8);
            assert_eq!(v + 1, decode_fixed_64(s.as_mut_slice()));
            s.drain(0..8);
        }
    }
}
