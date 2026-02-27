// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Exponential backoff utility.
//!
//! Replaces C++ `exponential_backoff.cc/h`.

use std::time::Duration;

/// Exponential backoff calculator with jitter.
pub struct ExponentialBackoff {
    initial_delay: Duration,
    multiplier: f64,
    max_delay: Duration,
    current_delay: Duration,
}

impl ExponentialBackoff {
    pub fn new(initial_delay: Duration, multiplier: f64, max_delay: Duration) -> Self {
        Self {
            initial_delay,
            multiplier,
            max_delay,
            current_delay: initial_delay,
        }
    }

    /// Returns the next backoff duration and advances the internal state.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current_delay;
        let next = Duration::from_secs_f64(self.current_delay.as_secs_f64() * self.multiplier);
        self.current_delay = next.min(self.max_delay);
        delay
    }

    /// Returns the next backoff duration with random jitter (0 to +50%).
    pub fn next_delay_with_jitter(&mut self) -> Duration {
        let delay = self.next_delay();
        let jitter_factor = 1.0 + rand::random::<f64>() * 0.5;
        Duration::from_secs_f64(delay.as_secs_f64() * jitter_factor).min(self.max_delay)
    }

    /// Reset the backoff to initial delay.
    pub fn reset(&mut self) {
        self.current_delay = self.initial_delay;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_growth() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(100), 2.0, Duration::from_secs(10));
        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));
        assert_eq!(backoff.next_delay(), Duration::from_millis(400));
    }

    #[test]
    fn test_max_cap() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_secs(5), 3.0, Duration::from_secs(10));
        assert_eq!(backoff.next_delay(), Duration::from_secs(5));
        // Next would be 15s but capped at 10s
        assert_eq!(backoff.next_delay(), Duration::from_secs(10));
        assert_eq!(backoff.next_delay(), Duration::from_secs(10));
    }

    #[test]
    fn test_reset() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(100), 2.0, Duration::from_secs(10));
        backoff.next_delay();
        backoff.next_delay();
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
    }
}
