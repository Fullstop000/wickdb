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

use super::slice::Slice;

/// Encodes `value` in little-endian and puts it in the first 4-bytes of `dst`.
///
/// # Panics
///
/// Panics if `dst.len()` is less than 4.
pub fn encode_fixed_32(dst: &mut [u8], value: u32) {
    invarint!(
        dst.len() >= 4,
        "the length of 'dst' must be more than 4 for a u32",
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
    invarint!(
        dst.len() >= 4,
        "the length of 'dst' must be more than 4 for a u64",
    );
    unsafe {
        let bytes = transmute::<u64, [u8; 8]>(value.to_le());
        copy_nonoverlapping(bytes.as_ptr(), dst.as_mut_ptr(), 8);
    }
}

/// Decodes the first 4-bytes of `src` in little-endian and returns the decoded value.
///
/// # Panics
///
/// Panics if `src.len()` is less than 4.
pub fn decode_fixed_32(src: &[u8]) -> u32 {
    invarint!(
        src.len() >= 4,
        "the length of 'src' must be more than 4 for converting to a u32",
    );
    let mut data: u32 = 0;
    unsafe {
        copy_nonoverlapping(src.as_ptr(), &mut data as *mut u32 as *mut u8, 4);
    }
    data.to_le()
}

/// Decodes the first 8-bytes of `src` in little-endian and returns the decoded value.
///
/// # Panics
///
/// Panics if `src.len()` is less than 8.
pub fn decode_fixed_64(src: &[u8]) -> u64 {
    invarint!(
        src.len() >= 8,
        "the length of 'src' must be more than 4 for converting to a u64",
    );
    let mut data: u64 = 0;
    unsafe {
        copy_nonoverlapping(src.as_ptr(), &mut data as *mut u64 as *mut u8, 8);
    }
    data.to_le()
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
            (vec![0, 0, 0, 0], 0u32),
            (vec![1, 0, 0, 0], 1u32),
            (vec![255, 0, 0, 0], 255u32),
            (vec![0, 1, 0, 0], 256u32),
            (vec![0, 2, 0, 0], 512u32),
            (vec![255, 255, 255, 255], u32::max_value()),
            (vec![255, 255, 255, 255, 0, 0], u32::max_value()),
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
            (vec![0; 8], 0u64),
            (vec![1, 0, 0, 0, 0, 0, 0, 0], 1u64),
            (vec![255, 0, 0, 0, 0, 0, 0, 0], 255u64),
            (vec![0, 1, 0, 0, 0, 0, 0, 0], 256u64),
            (vec![0, 2, 0, 0, 0, 0, 0, 0], 512u64),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255],
                u64::max_value(),
            ),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255, 0, 0],
                u64::max_value(),
            ),
        ];
        for (src, expect) in tests.drain(..) {
            let result = decode_fixed_64(src.as_slice());
            assert_eq!(result, expect);
        }
    }
}
