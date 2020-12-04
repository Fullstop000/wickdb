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

use crc32fast::Hasher;

const MASK_DELTA: u32 = 0xa282ead8;

/// Returns a `u32` crc checksum for give data
pub fn hash(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}

pub fn extend(crc: u32, data: &[u8]) -> u32 {
    let mut h = Hasher::new_with_initial(crc);
    h.update(data);
    h.finalize()
}

/// Return a masked representation of crc.
///
/// Motivation: it is problematic to compute the CRC of a string that
/// contains embedded CRCs.  Therefore we recommend that CRCs stored
/// somewhere (e.g., in files) should be masked before being stored.
pub fn mask(crc: u32) -> u32 {
    ((crc >> 15) | (crc << 17)).wrapping_add(MASK_DELTA)
}

/// Return the crc whose masked representation is `masked`.
pub fn unmask(masked: u32) -> u32 {
    let rot = masked.wrapping_sub(MASK_DELTA);
    (rot >> 17) | (rot << 15)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_standard_crc32_results() {
        let buf: Vec<u8> = vec![0; 32];
        assert_eq!(hash(&buf), 0x190a55ad);

        let mut buf: Vec<u8> = vec![0xff; 32];
        assert_eq!(hash(&buf), 0xff6cab0b);

        for i in 0..32 {
            buf[i] = i as u8;
        }
        assert_eq!(hash(&buf), 0x91267e8a);

        for i in 0..32 {
            buf[i] = (31 - i) as u8;
        }
        assert_eq!(hash(&buf), 0x9ab0ef72);

        let data = [
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(hash(&data), 0x51e17412);
    }

    #[test]
    pub fn test_values() {
        assert_ne!(hash(b"a"), hash(b"foo"));
    }

    #[test]
    pub fn test_extend() {
        assert_eq!(hash(b"hello world"), extend(hash(b"hello "), b"world"));
    }

    #[test]
    pub fn test_mask_unmask() {
        let crc = hash(b"foo");
        assert_ne!(mask(crc), crc);
        assert_ne!(mask(mask(crc)), crc);
        assert_eq!(unmask(mask(crc)), crc);
        assert_eq!(unmask(unmask(mask(mask(crc)))), crc);
    }
}
