// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Random number utilities.
//!
//! Replaces C++ `random.h`.

use rand::Rng;

/// Fill a byte slice with random bytes.
pub fn fill_random(buf: &mut [u8]) {
    rand::thread_rng().fill(buf);
}

/// Generate a random byte vector of the given length.
pub fn random_bytes(len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    fill_random(&mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fill_random() {
        let mut buf = [0u8; 32];
        fill_random(&mut buf);
        // Very unlikely that all 32 bytes are still zero
        assert!(buf.iter().any(|&b| b != 0));
    }
}
