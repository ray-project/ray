// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! RPC chaos testing framework.
//!
//! Replaces `src/ray/rpc/rpc_chaos.h/cc`.
//! Provides configurable failure injection for RPC methods, supporting
//! per-method and wildcard configurations with probabilistic and
//! lower-bound guaranteed failure modes.

use std::collections::HashMap;
use std::sync::Mutex;

use rand::Rng;

/// Type of RPC failure to inject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcFailure {
    /// No failure.
    None,
    /// Failure before server receives the request.
    Request,
    /// Failure after server sends the response.
    Response,
    /// Failure after server receives the request but before the response is sent.
    InFlight,
}

/// Valid keys in the JSON configuration for each method.
const VALID_KEYS: &[&str] = &[
    "num_failures",
    "req_failure_prob",
    "resp_failure_prob",
    "in_flight_failure_prob",
    "num_lower_bound_req_failures",
    "num_lower_bound_resp_failures",
    "num_lower_bound_in_flight_failures",
];

/// Configuration for a single failable method.
#[derive(Debug, Clone)]
struct Failable {
    num_remaining_failures: i64,
    req_failure_prob: u64,
    resp_failure_prob: u64,
    in_flight_failure_prob: u64,
    num_lower_bound_req_failures: u64,
    num_lower_bound_resp_failures: u64,
    num_lower_bound_in_flight_failures: u64,
}

/// RPC chaos failure manager.
///
/// Manages failure injection configuration and state for RPC methods.
/// Thread-safe via internal mutex.
pub struct RpcFailureManager {
    inner: Mutex<RpcFailureManagerInner>,
}

struct RpcFailureManagerInner {
    failable_methods: HashMap<String, Failable>,
    wildcard_set: bool,
    has_failures: bool,
    num_req_failures: HashMap<String, u64>,
    num_resp_failures: HashMap<String, u64>,
    num_in_flight_failures: HashMap<String, u64>,
    rng: rand::rngs::ThreadRng,
}

impl RpcFailureManager {
    /// Create a new empty failure manager.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(RpcFailureManagerInner {
                failable_methods: HashMap::new(),
                wildcard_set: false,
                has_failures: false,
                num_req_failures: HashMap::new(),
                num_resp_failures: HashMap::new(),
                num_in_flight_failures: HashMap::new(),
                rng: rand::thread_rng(),
            }),
        }
    }

    /// Initialize from a JSON configuration string.
    ///
    /// The JSON format is:
    /// ```json
    /// {
    ///   "method_name": {
    ///     "num_failures": 10,
    ///     "req_failure_prob": 25,
    ///     "resp_failure_prob": 25,
    ///     "in_flight_failure_prob": 25,
    ///     "num_lower_bound_req_failures": 0,
    ///     "num_lower_bound_resp_failures": 0,
    ///     "num_lower_bound_in_flight_failures": 0
    ///   }
    /// }
    /// ```
    ///
    /// Use `"*"` as method name for wildcard (applies to all methods).
    /// Use `num_failures: -1` for unlimited failures.
    ///
    /// # Panics
    ///
    /// Panics if the JSON contains unknown keys.
    pub fn init(&self, config_json: &str) {
        let mut inner = self.inner.lock().unwrap();

        // Clear old state.
        inner.failable_methods.clear();
        inner.num_req_failures.clear();
        inner.num_resp_failures.clear();
        inner.num_in_flight_failures.clear();
        inner.wildcard_set = false;
        inner.has_failures = false;

        if config_json.is_empty() {
            return;
        }

        let json: serde_json::Value =
            serde_json::from_str(config_json).expect("testing_rpc_failure is not valid JSON");

        let obj = json.as_object().expect("testing_rpc_failure must be a JSON object");

        for (method, config) in obj {
            let config_obj = config
                .as_object()
                .unwrap_or_else(|| panic!("Value for method '{method}' is not a JSON object"));

            // Validate keys.
            for key in config_obj.keys() {
                if !VALID_KEYS.contains(&key.as_str()) {
                    panic!(
                        "Unknown key specified in testing_rpc_failure config: {key}"
                    );
                }
            }

            let failable = Failable {
                num_remaining_failures: config_obj
                    .get("num_failures")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0),
                req_failure_prob: config_obj
                    .get("req_failure_prob")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                resp_failure_prob: config_obj
                    .get("resp_failure_prob")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                in_flight_failure_prob: config_obj
                    .get("in_flight_failure_prob")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                num_lower_bound_req_failures: config_obj
                    .get("num_lower_bound_req_failures")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                num_lower_bound_resp_failures: config_obj
                    .get("num_lower_bound_resp_failures")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                num_lower_bound_in_flight_failures: config_obj
                    .get("num_lower_bound_in_flight_failures")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
            };

            assert!(
                failable.req_failure_prob
                    + failable.resp_failure_prob
                    + failable.in_flight_failure_prob
                    <= 100,
                "Total failure probability exceeds 100%"
            );

            inner
                .failable_methods
                .insert(method.clone(), failable);

            if method == "*" {
                inner.wildcard_set = true;
                break;
            }
        }

        inner.has_failures = true;
    }

    /// Get the RPC failure type to inject for the given method name.
    pub fn get_rpc_failure(&self, name: &str) -> RpcFailure {
        let mut inner = self.inner.lock().unwrap();

        if !inner.has_failures {
            return RpcFailure::None;
        }

        if inner.wildcard_set {
            // Clone the failable to work around borrow checker.
            let mut failable = inner.failable_methods.get("*").unwrap().clone();
            let result =
                get_failure_type_from_failable(&mut failable, name, &mut inner);
            inner.failable_methods.insert("*".to_string(), failable);
            return result;
        }

        if let Some(mut failable) = inner.failable_methods.get(name).cloned() {
            let result =
                get_failure_type_from_failable(&mut failable, name, &mut inner);
            inner
                .failable_methods
                .insert(name.to_string(), failable);
            result
        } else {
            RpcFailure::None
        }
    }
}

impl Default for RpcFailureManager {
    fn default() -> Self {
        Self::new()
    }
}

fn get_failure_type_from_failable(
    failable: &mut Failable,
    name: &str,
    inner: &mut RpcFailureManagerInner,
) -> RpcFailure {
    if failable.num_remaining_failures == 0 {
        return RpcFailure::None;
    }

    let req_count = *inner.num_req_failures.get(name).unwrap_or(&0);
    if req_count < failable.num_lower_bound_req_failures {
        failable.num_remaining_failures -= 1;
        *inner.num_req_failures.entry(name.to_string()).or_insert(0) += 1;
        return RpcFailure::Request;
    }

    let resp_count = *inner.num_resp_failures.get(name).unwrap_or(&0);
    if resp_count < failable.num_lower_bound_resp_failures {
        failable.num_remaining_failures -= 1;
        *inner.num_resp_failures.entry(name.to_string()).or_insert(0) += 1;
        return RpcFailure::Response;
    }

    let in_flight_count = *inner.num_in_flight_failures.get(name).unwrap_or(&0);
    if in_flight_count < failable.num_lower_bound_in_flight_failures {
        failable.num_remaining_failures -= 1;
        *inner
            .num_in_flight_failures
            .entry(name.to_string())
            .or_insert(0) += 1;
        return RpcFailure::InFlight;
    }

    let random_number: u64 = inner.rng.gen_range(1..=100);

    if random_number <= failable.req_failure_prob {
        failable.num_remaining_failures -= 1;
        RpcFailure::Request
    } else if random_number <= failable.req_failure_prob + failable.resp_failure_prob {
        failable.num_remaining_failures -= 1;
        RpcFailure::Response
    } else if random_number
        <= failable.req_failure_prob
            + failable.resp_failure_prob
            + failable.in_flight_failure_prob
    {
        failable.num_remaining_failures -= 1;
        RpcFailure::InFlight
    } else {
        RpcFailure::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of C++ MethodRpcFailure test.
    /// Tests per-method failure configuration with limited failures.
    #[test]
    fn test_method_rpc_failure() {
        let mgr = RpcFailureManager::new();
        mgr.init(
            r#"{"method1":{"num_failures":0,"req_failure_prob":25,"resp_failure_prob":25,"in_flight_failure_prob":25},"method2":{"num_failures":1,"req_failure_prob":100,"resp_failure_prob":0,"in_flight_failure_prob":0}}"#,
        );

        // Unknown method returns None.
        assert_eq!(mgr.get_rpc_failure("unknown"), RpcFailure::None);

        // method1 has num_failures=0, so always returns None.
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::None);

        // method2 has at most 1 failure with 100% req probability.
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Request);
        // After the single failure is consumed, returns None.
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::None);
    }

    /// Port of C++ MethodRpcFailureEdgeCase test.
    /// Tests that 100% probability for a single failure type always returns that type.
    #[test]
    fn test_method_rpc_failure_edge_case() {
        let mgr = RpcFailureManager::new();
        mgr.init(
            r#"{"method1":{"num_failures":1000,"req_failure_prob":100,"resp_failure_prob":0,"in_flight_failure_prob":0},"method2":{"num_failures":1000,"req_failure_prob":0,"resp_failure_prob":100,"in_flight_failure_prob":0},"method3":{"num_failures":1000,"req_failure_prob":0,"resp_failure_prob":0,"in_flight_failure_prob":100},"method4":{"num_failures":1000,"req_failure_prob":0,"resp_failure_prob":0,"in_flight_failure_prob":0}}"#,
        );

        for _ in 0..1000 {
            assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Request);
            assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Response);
            assert_eq!(mgr.get_rpc_failure("method3"), RpcFailure::InFlight);
            assert_eq!(mgr.get_rpc_failure("method4"), RpcFailure::None);
        }
    }

    /// Port of C++ WildcardRpcFailure test.
    /// Tests wildcard configuration with different failure probabilities.
    #[test]
    fn test_wildcard_rpc_failure() {
        // 100% request failure for all methods.
        let mgr = RpcFailureManager::new();
        mgr.init(
            r#"{"*":{"num_failures":-1,"req_failure_prob":100,"resp_failure_prob":0,"in_flight_failure_prob":0}}"#,
        );
        for _ in 0..100 {
            assert_eq!(mgr.get_rpc_failure("method"), RpcFailure::Request);
        }

        // 100% response failure for all methods.
        let mgr = RpcFailureManager::new();
        mgr.init(
            r#"{"*":{"num_failures":-1,"req_failure_prob":0,"resp_failure_prob":100,"in_flight_failure_prob":0}}"#,
        );
        for _ in 0..100 {
            assert_eq!(mgr.get_rpc_failure("method"), RpcFailure::Response);
        }

        // 100% in-flight failure for all methods.
        let mgr = RpcFailureManager::new();
        mgr.init(
            r#"{"*":{"num_failures":-1,"req_failure_prob":0,"resp_failure_prob":0,"in_flight_failure_prob":100}}"#,
        );
        for _ in 0..100 {
            assert_eq!(mgr.get_rpc_failure("method"), RpcFailure::InFlight);
        }

        // 0% failure probability - all None.
        let mgr = RpcFailureManager::new();
        mgr.init(
            r#"{"*":{"num_failures":-1,"req_failure_prob":0,"resp_failure_prob":0,"in_flight_failure_prob":0}}"#,
        );
        for _ in 0..100 {
            assert_eq!(mgr.get_rpc_failure("method"), RpcFailure::None);
        }
    }

    /// Port of C++ LowerBoundWithWildcard test.
    /// Tests lower bound guaranteed failures followed by probabilistic failures.
    #[test]
    fn test_lower_bound_with_wildcard() {
        let mgr = RpcFailureManager::new();
        mgr.init(
            r#"{
                "*": {
                    "num_failures": -1,
                    "req_failure_prob": 100,
                    "resp_failure_prob": 0,
                    "in_flight_failure_prob": 0,
                    "num_lower_bound_req_failures": 3,
                    "num_lower_bound_resp_failures": 5,
                    "num_lower_bound_in_flight_failures": 2
                }
            }"#,
        );

        // First 3 calls for method1 should be guaranteed Request failures.
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Request);
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Request);
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Request);

        // Next 5 calls should be guaranteed Response failures.
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Response);
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Response);
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Response);
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Response);
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Response);

        // Next 2 calls should be guaranteed InFlight failures.
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::InFlight);
        assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::InFlight);

        // After lower bounds exhausted, 100% request failures (probabilistic).
        for _ in 0..100 {
            assert_eq!(mgr.get_rpc_failure("method1"), RpcFailure::Request);
        }

        // Wildcard applies to any method - method2 should have the same behavior.
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Request);
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Request);
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Request);

        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Response);
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Response);
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Response);
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Response);
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Response);

        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::InFlight);
        assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::InFlight);

        for _ in 0..100 {
            assert_eq!(mgr.get_rpc_failure("method2"), RpcFailure::Request);
        }
    }

    /// Port of C++ TestInvalidJson test.
    /// Tests that unknown keys in config cause a panic.
    #[test]
    #[should_panic(expected = "Unknown key specified in testing_rpc_failure config: invalid_key")]
    fn test_invalid_json() {
        let mgr = RpcFailureManager::new();
        mgr.init(r#"{"*":{"num_failures":-1,"invalid_key":1}}"#);
    }
}
