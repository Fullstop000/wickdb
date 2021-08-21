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

pub const MAX_VARINT_LEN_U32: usize = 5;
pub const MAX_VARINT_LEN_U64: usize = 10;

pub struct VarintU32;
pub struct VarintU64;

macro_rules! impl_varint {
    ($type:ty, $uint: ty) => {
        impl $type {
            /// Encodes a uint into given vec and returns the number of bytes written.
            /// Using little endian style.
            /// See Varint in https://developers.google.com/protocol-buffers/docs/encoding#varints
            ///
            /// # Panic
            ///
            /// Panic when `dst` length is not enough
            pub fn write(dst: &mut [u8], mut n: $uint) -> usize {
                let mut i = 0;
                while n >= 0b1000_0000 {
                    dst[i] = (n as u8) | 0b1000_0000;
                    n >>= 7;
                    i += 1;
                }
                dst[i] = n as u8;
                i + 1
            }

            /// Decodes a uint(32 or 64) from given bytes and returns that value and the
            /// number of bytes read ( > 0).
            /// If an error or overflow occurred, returns `None`
            pub fn read(src: &[u8]) -> Option<($uint, usize)> {
                let mut n: $uint = 0;
                let mut shift: u32 = 0;
                for (i, &b) in src.iter().enumerate() {
                    if b < 0b1000_0000 {
                        return (<$uint>::from(b))
                            .checked_shl(shift)
                            .map(|b| (n | b, (i + 1) as usize));
                    }
                    match ((<$uint>::from(b)) & 0b0111_1111).checked_shl(shift) {
                        None => return None,
                        Some(b) => n |= b,
                    }
                    shift += 7;
                }
                None
            }

            /// Append `n` as varint bytes into the dst.
            /// Returns the bytes written.
            pub fn put_varint(dst: &mut Vec<u8>, mut n: $uint) -> usize {
                let mut i = 0;
                while n >= 0b1000_0000 {
                    dst.push((n as u8) | 0b1000_0000);
                    n >>= 7;
                    i += 1;
                }
                dst.push(n as u8);
                i + 1
            }

            /// Encodes the slice `src` into the `dst` as varint length prefixed
            pub fn put_varint_prefixed_slice(dst: &mut Vec<u8>, src: &[u8]) {
                if !src.is_empty() {
                    Self::put_varint(dst, src.len() as $uint);
                    dst.extend_from_slice(src);
                }
            }

            /// Decodes the varint-length-prefixed slice from `src, and advance `src`
            pub fn get_varint_prefixed_slice<'a>(src: &mut &'a [u8]) -> Option<&'a [u8]> {
                Self::read(src).and_then(|(len, n)| {
                    let read_len = len as usize + n;
                    if read_len > src.len() {
                        return None;
                    }
                    let res = &src[n..read_len];
                    *src = &src[read_len..];
                    Some(res)
                })
            }

            /// Decodes a u64 from given bytes and returns that value and the
            /// number of bytes read ( > 0).If an error occurred, the value is 0
            /// and the number of bytes n is <= 0 meaning:
            ///
            ///  n == 0:buf too small
            ///  n  < 0: value larger than 64 bits (overflow)
            ///          and -n is the number of bytes read
            ///
            pub fn common_read(src: &[u8]) -> ($uint, isize) {
                let mut n: $uint = 0;
                let mut shift: u32 = 0;
                for (i, &b) in src.iter().enumerate() {
                    if b < 0b1000_0000 {
                        return match (<$uint>::from(b)).checked_shl(shift) {
                            None => (0, -(i as isize + 1)),
                            Some(b) => (n | b, (i + 1) as isize),
                        };
                    }
                    match ((<$uint>::from(b)) & 0b0111_1111).checked_shl(shift) {
                        None => return (0, -(i as isize)),
                        Some(b) => n |= b,
                    }
                    shift += 7;
                }
                (0, 0)
            }

            /// Decodes a uint from the give slice , and advance the given slice
            pub fn drain_read(src: &mut &[u8]) -> Option<$uint> {
                <$type>::read(src).and_then(|(v, n)| {
                    *src = &src[n..];
                    Some(v)
                })
            }
        }
    };
}

impl_varint!(VarintU32, u32);
impl_varint!(VarintU64, u64);

#[cfg(test)]
mod tests {
    use super::*;

    /*
       we just use VarintU64 here for testing because the implementation of VarintU32
       is as same as VarintU64
    */
    #[test]
    fn test_write_u64() {
        // (input u64 , expected bytes)
        let tests = vec![
            (0u64, vec![0]),
            (100u64, vec![0b110_0100]),
            (129u64, vec![0b1000_0001, 0b1]),
            (258u64, vec![0b1000_0010, 0b10]),
            (
                58962304u64,
                vec![0b1000_0000, 0b1110_0011, 0b1000_1110, 0b1_1100],
            ),
        ];
        for (input, results) in tests {
            let mut bytes = Vec::with_capacity(MAX_VARINT_LEN_U64);
            // allocate index
            for _ in 0..results.len() {
                bytes.push(0);
            }
            let written = VarintU64::write(&mut bytes, input);
            assert_eq!(written, results.len());
            for (i, b) in bytes.iter().enumerate() {
                assert_eq!(results[i], *b);
            }
        }
    }

    #[test]
    fn test_read_u64() {
        #[rustfmt::skip]
        let mut test_data = vec![
            0,
            0b110_0100,
            0b1000_0001, 0b1,
            0b1000_0010, 0b10,
            0b1000_0000, 0b1110_0011, 0b1000_1110, 0b1_1100,
            0b1100_1110, 0b1000_0001, 0b1011_0101, 0b1101_1001, 0b1111_0110, 0b1010_1100, 0b1100_1110, 0b1000_0001, 0b1011_0101, 0b1101_1001, 0b1111_0110, 0b1010_1100,
        ];
        let expects = vec![
            Some((0u64, 1)),
            Some((100u64, 1)),
            Some((129u64, 2)),
            Some((258u64, 2)),
            Some((58962304u64, 4)),
            None,
        ];
        let mut idx = 0;
        while !test_data.is_empty() {
            match VarintU64::read(&test_data.as_slice()) {
                Some((i, n)) => {
                    assert_eq!(Some((i, n)), expects[idx]);
                    test_data.drain(0..n);
                }
                None => {
                    assert_eq!(None, expects[idx]);
                    test_data.drain(..);
                }
            }
            idx += 1;
        }
    }

    #[test]
    fn test_put_and_get_varint() {
        let mut buf = vec![];
        let mut numbers = vec![];
        let n = 100;
        for _ in 0..n {
            let r = rand::random::<u64>();
            VarintU64::put_varint(&mut buf, r);
            numbers.push(r);
        }
        let mut start = 0;
        for i in 0..n {
            if let Some((res, n)) = VarintU64::read(&buf.as_slice()[start..]) {
                assert_eq!(numbers[i], res);
                start += n
            }
        }
    }

    #[test]
    fn test_put_and_get_prefixed_slice() {
        let mut encoded = vec![];
        let tests: Vec<Vec<u8>> = vec![vec![1], vec![1, 2, 3, 4, 5], vec![0; 100]];
        for input in tests.clone() {
            VarintU64::put_varint_prefixed_slice(&mut encoded, &input);
        }
        let mut s = encoded.as_slice();
        let mut decoded = vec![];
        while s.len() > 0 {
            match VarintU64::get_varint_prefixed_slice(&mut s) {
                Some(res) => decoded.push(res.to_owned()),
                None => break,
            }
        }
        assert_eq!(tests.len(), decoded.len());
        for (get, want) in decoded.into_iter().zip(tests.into_iter()) {
            assert_eq!(get.len(), want.len());
            for (getv, wantv) in get.iter().zip(want.iter()) {
                assert_eq!(*getv, *wantv)
            }
        }
    }
}
