// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Runtime environment support for tasks and actors.
//!
//! A runtime environment specifies the working directory, environment variables,
//! pip packages, conda environments, and other configuration that should be
//! set up before a task or actor executes.
//!
//! Replaces parts of `src/ray/core_worker/core_worker.cc` related to
//! runtime environments and `src/ray/runtime_env/`.

use std::collections::HashMap;
use std::path::PathBuf;

/// A runtime environment specification.
#[derive(Debug, Clone, Default)]
pub struct RuntimeEnv {
    /// Working directory for the task.
    pub working_dir: Option<String>,
    /// Environment variables to set.
    pub env_vars: HashMap<String, String>,
    /// Pip packages to install.
    pub pip_packages: Vec<String>,
    /// Conda environment name or path.
    pub conda_env: Option<String>,
    /// Container image to use.
    pub container_image: Option<String>,
    /// Serialized runtime env protobuf (for pass-through).
    pub serialized_runtime_env: String,
    /// Hash of the runtime env for caching.
    pub runtime_env_hash: u64,
}

impl RuntimeEnv {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from serialized protobuf string.
    pub fn from_serialized(serialized: String) -> Self {
        let hash = hash_runtime_env(&serialized);
        Self {
            serialized_runtime_env: serialized,
            runtime_env_hash: hash,
            ..Default::default()
        }
    }

    /// Check if this is an empty/default runtime env.
    pub fn is_empty(&self) -> bool {
        self.working_dir.is_none()
            && self.env_vars.is_empty()
            && self.pip_packages.is_empty()
            && self.conda_env.is_none()
            && self.container_image.is_none()
            && self.serialized_runtime_env.is_empty()
    }

    /// Get the hash for caching purposes.
    pub fn hash(&self) -> u64 {
        self.runtime_env_hash
    }
}

/// A runtime environment context that manages setup and teardown.
pub struct RuntimeEnvContext {
    /// The resolved working directory path.
    pub working_dir: Option<PathBuf>,
    /// Environment variables that were set (for cleanup).
    pub env_vars_set: Vec<String>,
    /// Whether the runtime env is currently active.
    pub is_active: bool,
}

impl RuntimeEnvContext {
    pub fn new() -> Self {
        Self {
            working_dir: None,
            env_vars_set: Vec::new(),
            is_active: false,
        }
    }

    /// Set up the runtime environment.
    ///
    /// - Sets environment variables
    /// - Sets working directory
    pub fn setup(&mut self, env: &RuntimeEnv) -> Result<(), RuntimeEnvError> {
        // Set environment variables.
        for (key, value) in &env.env_vars {
            std::env::set_var(key, value);
            self.env_vars_set.push(key.clone());
        }

        // Set working directory.
        if let Some(ref dir) = env.working_dir {
            let path = PathBuf::from(dir);
            if !path.exists() {
                return Err(RuntimeEnvError::WorkingDirNotFound(dir.clone()));
            }
            std::env::set_current_dir(&path).map_err(|e| {
                RuntimeEnvError::SetupFailed(format!("failed to set working dir: {}", e))
            })?;
            self.working_dir = Some(path);
        }

        self.is_active = true;
        Ok(())
    }

    /// Tear down the runtime environment.
    ///
    /// - Removes environment variables that were set
    pub fn teardown(&mut self) {
        for key in self.env_vars_set.drain(..) {
            std::env::remove_var(&key);
        }
        self.is_active = false;
    }
}

impl Default for RuntimeEnvContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for RuntimeEnvContext {
    fn drop(&mut self) {
        if self.is_active {
            self.teardown();
        }
    }
}

/// Runtime environment errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RuntimeEnvError {
    #[error("working directory not found: {0}")]
    WorkingDirNotFound(String),
    #[error("runtime env setup failed: {0}")]
    SetupFailed(String),
    #[error("pip install failed: {0}")]
    PipInstallFailed(String),
    #[error("conda env not found: {0}")]
    CondaEnvNotFound(String),
}

/// A cache for runtime environments, keyed by hash.
pub struct RuntimeEnvCache {
    /// Map from runtime env hash → whether it's been set up.
    cache: parking_lot::Mutex<HashMap<u64, bool>>,
}

impl RuntimeEnvCache {
    pub fn new() -> Self {
        Self {
            cache: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Check if a runtime env has been set up.
    pub fn is_setup(&self, hash: u64) -> bool {
        self.cache.lock().get(&hash).copied().unwrap_or(false)
    }

    /// Mark a runtime env as set up.
    pub fn mark_setup(&self, hash: u64) {
        self.cache.lock().insert(hash, true);
    }

    /// Remove a runtime env from the cache.
    pub fn remove(&self, hash: u64) {
        self.cache.lock().remove(&hash);
    }

    /// Number of cached entries.
    pub fn size(&self) -> usize {
        self.cache.lock().len()
    }
}

impl Default for RuntimeEnvCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple hash function for runtime env strings.
fn hash_runtime_env(serialized: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    serialized.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_env_empty() {
        let env = RuntimeEnv::new();
        assert!(env.is_empty());
        assert_eq!(env.hash(), 0);
    }

    #[test]
    fn test_runtime_env_from_serialized() {
        let env = RuntimeEnv::from_serialized("{\"pip\": [\"numpy\"]}".into());
        assert!(!env.is_empty());
        assert_ne!(env.hash(), 0);
    }

    #[test]
    fn test_runtime_env_with_env_vars() {
        let mut env = RuntimeEnv::new();
        env.env_vars
            .insert("MY_VAR".to_string(), "my_value".to_string());
        assert!(!env.is_empty());
    }

    #[test]
    fn test_runtime_env_context_setup_teardown() {
        let mut env = RuntimeEnv::new();
        env.env_vars
            .insert("RAY_TEST_RUNTIME_ENV".to_string(), "42".to_string());

        let mut ctx = RuntimeEnvContext::new();
        ctx.setup(&env).unwrap();

        assert!(ctx.is_active);
        assert_eq!(std::env::var("RAY_TEST_RUNTIME_ENV").unwrap(), "42");

        ctx.teardown();
        assert!(!ctx.is_active);
        assert!(std::env::var("RAY_TEST_RUNTIME_ENV").is_err());
    }

    #[test]
    fn test_runtime_env_context_drop_cleans_up() {
        let mut env = RuntimeEnv::new();
        env.env_vars
            .insert("RAY_TEST_DROP".to_string(), "yes".to_string());

        {
            let mut ctx = RuntimeEnvContext::new();
            ctx.setup(&env).unwrap();
            assert_eq!(std::env::var("RAY_TEST_DROP").unwrap(), "yes");
            // ctx goes out of scope here, triggering drop → teardown
        }

        assert!(std::env::var("RAY_TEST_DROP").is_err());
    }

    #[test]
    fn test_runtime_env_cache() {
        let cache = RuntimeEnvCache::new();
        assert!(!cache.is_setup(42));
        assert_eq!(cache.size(), 0);

        cache.mark_setup(42);
        assert!(cache.is_setup(42));
        assert_eq!(cache.size(), 1);

        cache.remove(42);
        assert!(!cache.is_setup(42));
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_working_dir_not_found() {
        let mut env = RuntimeEnv::new();
        env.working_dir = Some("/nonexistent/ray/test/path".into());

        let mut ctx = RuntimeEnvContext::new();
        let err = ctx.setup(&env).unwrap_err();
        assert!(matches!(err, RuntimeEnvError::WorkingDirNotFound(_)));
    }
}
