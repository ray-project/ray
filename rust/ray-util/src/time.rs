// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Time utilities.
//!
//! Replaces C++ `time.cc/h`.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Get the current time in milliseconds since the Unix epoch.
pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Get the current time in nanoseconds since the Unix epoch.
pub fn current_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// A simple monotonic stopwatch for measuring elapsed time.
pub struct Stopwatch {
    start: Instant,
}

impl Default for Stopwatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Stopwatch {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.elapsed().as_millis() as u64
    }

    pub fn reset(&mut self) {
        self.start = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_time_ms() {
        let t1 = current_time_ms();
        let t2 = current_time_ms();
        assert!(t2 >= t1);
        // Sanity check: should be after year 2020
        assert!(t1 > 1_577_836_800_000);
    }

    #[test]
    fn test_stopwatch() {
        let sw = Stopwatch::new();
        std::thread::sleep(Duration::from_millis(10));
        assert!(sw.elapsed_ms() >= 10);
    }
}
