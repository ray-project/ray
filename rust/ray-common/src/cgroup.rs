// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Cgroup v2 management for worker resource isolation.
//!
//! Replaces `src/ray/common/cgroup2/` (10 files, ~4500 lines C++).
//!
//! Ray uses cgroups to enforce CPU and memory limits for worker processes.
//! This module provides:
//! - `CgroupManager` trait — abstract interface for cgroup operations
//! - `SysfsCgroupDriver` — Linux sysfs-based cgroup v2 driver
//! - `NoopCgroupManager` — no-op for non-Linux platforms or when disabled
//! - `CgroupSetup` — hierarchy creation for Ray's cgroup structure

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

/// Resource constraints for a cgroup.
#[derive(Debug, Clone, Default)]
pub struct CgroupResourceConstraints {
    /// CPU weight (1-10000, default 100). Higher = more CPU share.
    pub cpu_weight: Option<u32>,
    /// Memory minimum guarantee in bytes.
    pub memory_min: Option<u64>,
    /// Memory low watermark (best-effort) in bytes.
    pub memory_low: Option<u64>,
    /// Memory high watermark (throttle) in bytes.
    pub memory_high: Option<u64>,
    /// Memory hard limit in bytes.
    pub memory_max: Option<u64>,
}

/// The cgroup hierarchy structure used by Ray.
///
/// ```text
/// ray-node_{node_id}/
///   ├── system/      (raylet, GCS, dashboard)
///   ├── user/
///   │   └── workers/ (task/actor workers)
///   └── non-ray/     (non-ray processes)
/// ```
#[derive(Debug, Clone)]
pub struct CgroupHierarchy {
    /// Root cgroup path for this Ray node.
    pub root: PathBuf,
    /// System cgroup (raylet processes).
    pub system: PathBuf,
    /// Workers cgroup (task/actor processes).
    pub workers: PathBuf,
    /// Non-Ray processes cgroup.
    pub non_ray: PathBuf,
}

impl CgroupHierarchy {
    /// Create the standard Ray cgroup hierarchy paths.
    pub fn new(base_path: &Path, node_id: &str) -> Self {
        let root = base_path.join(format!("ray-node_{node_id}"));
        let system = root.join("system");
        let workers = root.join("user").join("workers");
        let non_ray = root.join("non-ray");
        Self {
            root,
            system,
            workers,
            non_ray,
        }
    }
}

/// Abstract interface for cgroup operations.
///
/// Implement this trait for platform-specific cgroup drivers.
pub trait CgroupManager: Send + Sync {
    /// Initialize the cgroup hierarchy. Creates directories and enables controllers.
    fn initialize(&self) -> io::Result<()>;

    /// Add a process to the workers cgroup.
    fn add_process_to_workers_cgroup(&self, pid: u32) -> io::Result<()>;

    /// Add a process to the system cgroup.
    fn add_process_to_system_cgroup(&self, pid: u32) -> io::Result<()>;

    /// Apply resource constraints to the workers cgroup.
    fn set_worker_constraints(&self, constraints: &CgroupResourceConstraints) -> io::Result<()>;

    /// Apply resource constraints to the system cgroup.
    fn set_system_constraints(&self, constraints: &CgroupResourceConstraints) -> io::Result<()>;

    /// Check if cgroup v2 is available on this system.
    fn is_cgroup_v2_available(&self) -> bool;

    /// Get the hierarchy paths.
    fn hierarchy(&self) -> &CgroupHierarchy;
}

/// No-op cgroup manager for non-Linux systems or when cgroups are disabled.
pub struct NoopCgroupManager {
    hierarchy: CgroupHierarchy,
}

impl NoopCgroupManager {
    pub fn new(node_id: &str) -> Self {
        Self {
            hierarchy: CgroupHierarchy::new(Path::new("/sys/fs/cgroup"), node_id),
        }
    }
}

impl CgroupManager for NoopCgroupManager {
    fn initialize(&self) -> io::Result<()> {
        Ok(())
    }

    fn add_process_to_workers_cgroup(&self, _pid: u32) -> io::Result<()> {
        Ok(())
    }

    fn add_process_to_system_cgroup(&self, _pid: u32) -> io::Result<()> {
        Ok(())
    }

    fn set_worker_constraints(&self, _constraints: &CgroupResourceConstraints) -> io::Result<()> {
        Ok(())
    }

    fn set_system_constraints(&self, _constraints: &CgroupResourceConstraints) -> io::Result<()> {
        Ok(())
    }

    fn is_cgroup_v2_available(&self) -> bool {
        false
    }

    fn hierarchy(&self) -> &CgroupHierarchy {
        &self.hierarchy
    }
}

/// Sysfs-based cgroup v2 driver for Linux.
///
/// Reads/writes cgroup control files under `/sys/fs/cgroup/`.
/// Only active on Linux; other platforms use `NoopCgroupManager`.
pub struct SysfsCgroupDriver {
    hierarchy: CgroupHierarchy,
}

impl SysfsCgroupDriver {
    pub fn new(base_path: &Path, node_id: &str) -> Self {
        Self {
            hierarchy: CgroupHierarchy::new(base_path, node_id),
        }
    }

    /// Create a cgroup directory if it doesn't exist.
    fn create_cgroup_dir(path: &Path) -> io::Result<()> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        Ok(())
    }

    /// Enable cpu and memory controllers in the subtree_control file.
    fn enable_controllers(cgroup_path: &Path) -> io::Result<()> {
        let control_file = cgroup_path.join("cgroup.subtree_control");
        std::fs::write(&control_file, "+cpu +memory")?;
        Ok(())
    }

    /// Move a process to a cgroup by writing its PID to cgroup.procs.
    fn move_process(cgroup_path: &Path, pid: u32) -> io::Result<()> {
        let procs_file = cgroup_path.join("cgroup.procs");
        std::fs::write(&procs_file, pid.to_string())?;
        Ok(())
    }

    /// Apply resource constraints to a cgroup directory.
    fn apply_constraints(
        cgroup_path: &Path,
        constraints: &CgroupResourceConstraints,
    ) -> io::Result<()> {
        if let Some(weight) = constraints.cpu_weight {
            let weight = weight.clamp(1, 10000);
            std::fs::write(cgroup_path.join("cpu.weight"), weight.to_string())?;
        }
        if let Some(min) = constraints.memory_min {
            std::fs::write(cgroup_path.join("memory.min"), min.to_string())?;
        }
        if let Some(low) = constraints.memory_low {
            std::fs::write(cgroup_path.join("memory.low"), low.to_string())?;
        }
        if let Some(high) = constraints.memory_high {
            std::fs::write(cgroup_path.join("memory.high"), high.to_string())?;
        }
        if let Some(max) = constraints.memory_max {
            std::fs::write(cgroup_path.join("memory.max"), max.to_string())?;
        }
        Ok(())
    }
}

impl CgroupManager for SysfsCgroupDriver {
    fn initialize(&self) -> io::Result<()> {
        // Create the hierarchy directories
        Self::create_cgroup_dir(&self.hierarchy.root)?;
        Self::create_cgroup_dir(&self.hierarchy.system)?;
        Self::create_cgroup_dir(self.hierarchy.workers.parent().unwrap())?; // user/
        Self::create_cgroup_dir(&self.hierarchy.workers)?;
        Self::create_cgroup_dir(&self.hierarchy.non_ray)?;

        // Enable controllers at each level
        Self::enable_controllers(&self.hierarchy.root)?;
        Self::enable_controllers(self.hierarchy.workers.parent().unwrap())?;

        Ok(())
    }

    fn add_process_to_workers_cgroup(&self, pid: u32) -> io::Result<()> {
        Self::move_process(&self.hierarchy.workers, pid)
    }

    fn add_process_to_system_cgroup(&self, pid: u32) -> io::Result<()> {
        Self::move_process(&self.hierarchy.system, pid)
    }

    fn set_worker_constraints(&self, constraints: &CgroupResourceConstraints) -> io::Result<()> {
        Self::apply_constraints(&self.hierarchy.workers, constraints)
    }

    fn set_system_constraints(&self, constraints: &CgroupResourceConstraints) -> io::Result<()> {
        Self::apply_constraints(&self.hierarchy.system, constraints)
    }

    fn is_cgroup_v2_available(&self) -> bool {
        Path::new("/sys/fs/cgroup/cgroup.controllers").exists()
    }

    fn hierarchy(&self) -> &CgroupHierarchy {
        &self.hierarchy
    }
}

/// Create a cgroup manager appropriate for the current platform.
pub fn create_cgroup_manager(
    node_id: &str,
    enabled: bool,
) -> Box<dyn CgroupManager> {
    if !enabled {
        return Box::new(NoopCgroupManager::new(node_id));
    }

    #[cfg(target_os = "linux")]
    {
        let driver = SysfsCgroupDriver::new(Path::new("/sys/fs/cgroup"), node_id);
        if driver.is_cgroup_v2_available() {
            return Box::new(driver);
        }
    }

    Box::new(NoopCgroupManager::new(node_id))
}

/// Read cgroup resource stats from a cgroup directory.
///
/// Returns a map of stat name → value (e.g., "memory.current" → bytes).
pub fn read_cgroup_stats(cgroup_path: &Path) -> HashMap<String, String> {
    let mut stats = HashMap::new();
    let files = ["memory.current", "memory.max", "cpu.stat", "cpu.weight"];
    for file in &files {
        let path = cgroup_path.join(file);
        if let Ok(content) = std::fs::read_to_string(&path) {
            stats.insert(file.to_string(), content.trim().to_string());
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cgroup_hierarchy_paths() {
        let h = CgroupHierarchy::new(Path::new("/sys/fs/cgroup"), "abc123");
        assert_eq!(h.root, PathBuf::from("/sys/fs/cgroup/ray-node_abc123"));
        assert_eq!(
            h.system,
            PathBuf::from("/sys/fs/cgroup/ray-node_abc123/system")
        );
        assert_eq!(
            h.workers,
            PathBuf::from("/sys/fs/cgroup/ray-node_abc123/user/workers")
        );
        assert_eq!(
            h.non_ray,
            PathBuf::from("/sys/fs/cgroup/ray-node_abc123/non-ray")
        );
    }

    #[test]
    fn test_noop_manager_is_safe() {
        let mgr = NoopCgroupManager::new("test-node");
        assert!(mgr.initialize().is_ok());
        assert!(mgr.add_process_to_workers_cgroup(1234).is_ok());
        assert!(mgr.add_process_to_system_cgroup(5678).is_ok());
        assert!(!mgr.is_cgroup_v2_available());
    }

    #[test]
    fn test_noop_set_constraints() {
        let mgr = NoopCgroupManager::new("test");
        let constraints = CgroupResourceConstraints {
            cpu_weight: Some(200),
            memory_max: Some(1024 * 1024 * 1024),
            ..Default::default()
        };
        assert!(mgr.set_worker_constraints(&constraints).is_ok());
        assert!(mgr.set_system_constraints(&constraints).is_ok());
    }

    #[test]
    fn test_create_cgroup_manager_disabled() {
        let mgr = create_cgroup_manager("node1", false);
        assert!(!mgr.is_cgroup_v2_available());
        assert!(mgr.initialize().is_ok());
    }

    #[test]
    fn test_resource_constraints_default() {
        let c = CgroupResourceConstraints::default();
        assert!(c.cpu_weight.is_none());
        assert!(c.memory_min.is_none());
        assert!(c.memory_low.is_none());
        assert!(c.memory_high.is_none());
        assert!(c.memory_max.is_none());
    }

    #[test]
    fn test_sysfs_driver_with_temp_dir() {
        let dir = tempfile::tempdir().unwrap();
        let driver = SysfsCgroupDriver::new(dir.path(), "test-node");

        // Create the hierarchy directories
        let h = driver.hierarchy();
        SysfsCgroupDriver::create_cgroup_dir(&h.root).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.system).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(h.workers.parent().unwrap()).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.workers).unwrap();

        // Write a PID to workers cgroup.procs
        let procs = h.workers.join("cgroup.procs");
        SysfsCgroupDriver::move_process(&h.workers, 12345).unwrap();
        assert_eq!(std::fs::read_to_string(&procs).unwrap(), "12345");
    }

    #[test]
    fn test_sysfs_apply_constraints() {
        let dir = tempfile::tempdir().unwrap();
        let cgroup_dir = dir.path().join("test-cgroup");
        std::fs::create_dir_all(&cgroup_dir).unwrap();

        let constraints = CgroupResourceConstraints {
            cpu_weight: Some(500),
            memory_min: Some(1024),
            memory_max: Some(1024 * 1024),
            ..Default::default()
        };
        SysfsCgroupDriver::apply_constraints(&cgroup_dir, &constraints).unwrap();

        assert_eq!(
            std::fs::read_to_string(cgroup_dir.join("cpu.weight")).unwrap(),
            "500"
        );
        assert_eq!(
            std::fs::read_to_string(cgroup_dir.join("memory.min")).unwrap(),
            "1024"
        );
        assert_eq!(
            std::fs::read_to_string(cgroup_dir.join("memory.max")).unwrap(),
            "1048576"
        );
        // memory.low and memory.high should not be written (None)
        assert!(!cgroup_dir.join("memory.low").exists());
        assert!(!cgroup_dir.join("memory.high").exists());
    }

    #[test]
    fn test_cpu_weight_clamping() {
        let dir = tempfile::tempdir().unwrap();
        let cgroup_dir = dir.path().join("clamp-test");
        std::fs::create_dir_all(&cgroup_dir).unwrap();

        // Weight 0 should be clamped to 1
        let constraints = CgroupResourceConstraints {
            cpu_weight: Some(0),
            ..Default::default()
        };
        SysfsCgroupDriver::apply_constraints(&cgroup_dir, &constraints).unwrap();
        assert_eq!(
            std::fs::read_to_string(cgroup_dir.join("cpu.weight")).unwrap(),
            "1"
        );

        // Weight 99999 should be clamped to 10000
        let constraints = CgroupResourceConstraints {
            cpu_weight: Some(99999),
            ..Default::default()
        };
        SysfsCgroupDriver::apply_constraints(&cgroup_dir, &constraints).unwrap();
        assert_eq!(
            std::fs::read_to_string(cgroup_dir.join("cpu.weight")).unwrap(),
            "10000"
        );
    }

    #[test]
    fn test_read_cgroup_stats() {
        let dir = tempfile::tempdir().unwrap();
        let cgroup_dir = dir.path().join("stats-test");
        std::fs::create_dir_all(&cgroup_dir).unwrap();

        std::fs::write(cgroup_dir.join("memory.current"), "4096\n").unwrap();
        std::fs::write(cgroup_dir.join("cpu.weight"), "100\n").unwrap();

        let stats = read_cgroup_stats(&cgroup_dir);
        assert_eq!(stats.get("memory.current"), Some(&"4096".to_string()));
        assert_eq!(stats.get("cpu.weight"), Some(&"100".to_string()));
        assert!(stats.get("memory.max").is_none()); // File doesn't exist
    }

    #[test]
    fn test_hierarchy_trait_object() {
        let mgr: Box<dyn CgroupManager> = Box::new(NoopCgroupManager::new("n1"));
        assert!(!mgr.is_cgroup_v2_available());
        let h = mgr.hierarchy();
        assert!(h.root.to_str().unwrap().contains("ray-node_n1"));
    }
}
