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

use libc::{c_int, c_void, size_t};
use std::cmp::{min, Ordering};

extern "C" {
    fn memcmp(cx: *const c_void, ct: *const c_void, n: size_t) -> c_int;
}

#[inline]
pub fn compare(b1: &[u8], b2: &[u8]) -> Ordering {
    if b1.is_empty() && b2.is_empty() {
        return Ordering::Equal;
    }
    if b1.is_empty() {
        return Ordering::Less;
    }
    if b2.is_empty() {
        return Ordering::Greater;
    }
    let n = min(b1.len(), b2.len());
    unsafe {
        let result = memcmp(
            b1.as_ptr() as *const c_void,
            b2.as_ptr() as *const c_void,
            n as size_t,
        );
        match result {
            -1 => Ordering::Less,
            0 => Ordering::Equal,
            1 => Ordering::Greater,
            _ => panic!("invalid memcmp returning [{}]", result),
        }
    }
}

mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn test_compare() {
        let mut tests = vec![
            (vec![], vec![], Ordering::Equal),
            (vec![], vec![1u8], Ordering::Less),
            (vec![2u8], vec![], Ordering::Greater),
            (vec![1u8, 2u8, 3u8], vec![1u8, 2u8, 3u8], Ordering::Equal),
            (vec![1u8, 2u8, 3u8], vec![1u8, 3u8, 2u8], Ordering::Less),
            (vec![1u8, 3u8, 3u8], vec![1u8, 2u8, 2u8], Ordering::Greater),
        ];

        for (b1, b2, expect) in tests.drain(..) {
            assert_eq!(compare(b1.as_slice(), b2.as_slice()), expect);
        }
    }

}
