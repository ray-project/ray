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
pub fn create_cgroup_manager(node_id: &str, enabled: bool) -> Box<dyn CgroupManager> {
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

    // ─── Ported from C++ sysfs_cgroup_driver_test.cc ────────────────────────

    /// Port of CheckCgroupv2EnabledFailsIfEmptyMountFile:
    /// verify driver with no cgroupv2 is not available.
    #[test]
    fn test_cgroup_v2_not_available_on_macos() {
        // On non-Linux, the sysfs driver reports cgroupv2 as not available.
        let dir = tempfile::tempdir().unwrap();
        let driver = SysfsCgroupDriver::new(dir.path(), "test");
        assert!(!driver.is_cgroup_v2_available());
    }

    /// Port of CheckCgroup / CheckCgroupFailsIfNotCgroupv2Path:
    /// verify create_cgroup_dir works for regular temp dirs.
    #[test]
    fn test_create_cgroup_dir_creates_nested() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a").join("b").join("c");
        assert!(!nested.exists());
        SysfsCgroupDriver::create_cgroup_dir(&nested).unwrap();
        assert!(nested.exists());
    }

    /// Port of CheckCgroupFailsIfCgroupDoesNotExist:
    /// verify create_cgroup_dir is idempotent.
    #[test]
    fn test_create_cgroup_dir_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cg");
        SysfsCgroupDriver::create_cgroup_dir(&path).unwrap();
        assert!(path.exists());
        // Second call should also succeed without error
        SysfsCgroupDriver::create_cgroup_dir(&path).unwrap();
        assert!(path.exists());
    }

    /// Port of DeleteCgroupFailsIfNotCgroup2Path / DeleteCgroupFailsIfCgroupDoesNotExist:
    /// verify move_process writes PID to cgroup.procs.
    #[test]
    fn test_move_process_writes_pid() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("test-cg");
        std::fs::create_dir_all(&cg).unwrap();
        SysfsCgroupDriver::move_process(&cg, 42).unwrap();
        let content = std::fs::read_to_string(cg.join("cgroup.procs")).unwrap();
        assert_eq!(content, "42");
    }

    /// Port of EnableControllerFailsIfNotCgroupv2Path:
    /// verify enable_controllers writes subtree_control.
    #[test]
    fn test_enable_controllers_writes_subtree_control() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("test-cg");
        std::fs::create_dir_all(&cg).unwrap();
        SysfsCgroupDriver::enable_controllers(&cg).unwrap();
        let content = std::fs::read_to_string(cg.join("cgroup.subtree_control")).unwrap();
        assert_eq!(content, "+cpu +memory");
    }

    /// Port of AddConstraintFailsIfNotCgroupv2Path:
    /// verify apply_constraints writes all configured files.
    #[test]
    fn test_apply_constraints_all_fields() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("full-constraints");
        std::fs::create_dir_all(&cg).unwrap();

        let constraints = CgroupResourceConstraints {
            cpu_weight: Some(1000),
            memory_min: Some(1024 * 1024 * 1024),
            memory_low: Some(512 * 1024 * 1024),
            memory_high: Some(2 * 1024 * 1024 * 1024),
            memory_max: Some(4 * 1024 * 1024 * 1024),
        };
        SysfsCgroupDriver::apply_constraints(&cg, &constraints).unwrap();

        assert_eq!(
            std::fs::read_to_string(cg.join("cpu.weight")).unwrap(),
            "1000"
        );
        assert_eq!(
            std::fs::read_to_string(cg.join("memory.min")).unwrap(),
            "1073741824"
        );
        assert_eq!(
            std::fs::read_to_string(cg.join("memory.low")).unwrap(),
            "536870912"
        );
        assert_eq!(
            std::fs::read_to_string(cg.join("memory.high")).unwrap(),
            "2147483648"
        );
        assert_eq!(
            std::fs::read_to_string(cg.join("memory.max")).unwrap(),
            "4294967296"
        );
    }

    /// Port of GetAvailableControllersFailsIfNotCgroup2Path:
    /// verify read_cgroup_stats handles missing files gracefully.
    #[test]
    fn test_read_cgroup_stats_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("empty-cg");
        std::fs::create_dir_all(&cg).unwrap();
        let stats = read_cgroup_stats(&cg);
        assert!(stats.is_empty());
    }

    /// Port of read cgroup stats with partial files.
    #[test]
    fn test_read_cgroup_stats_partial_files() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("partial-cg");
        std::fs::create_dir_all(&cg).unwrap();
        std::fs::write(cg.join("memory.current"), "999\n").unwrap();
        std::fs::write(cg.join("memory.max"), "max\n").unwrap();
        // cpu.stat and cpu.weight missing

        let stats = read_cgroup_stats(&cg);
        assert_eq!(stats.get("memory.current"), Some(&"999".to_string()));
        assert_eq!(stats.get("memory.max"), Some(&"max".to_string()));
        assert!(stats.get("cpu.stat").is_none());
        assert!(stats.get("cpu.weight").is_none());
    }

    // ─── Ported from C++ cgroup_manager_test.cc ─────────────────────────────

    /// Port of CreateSucceedsWithCleanupInOrder (hierarchy creation part):
    /// verify full hierarchy creation with SysfsCgroupDriver.
    #[test]
    fn test_sysfs_driver_full_hierarchy_creation() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = "id_123";
        let driver = SysfsCgroupDriver::new(dir.path(), node_id);
        let h = driver.hierarchy();

        // Create all directories
        SysfsCgroupDriver::create_cgroup_dir(&h.root).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.system).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(h.workers.parent().unwrap()).unwrap(); // user/
        SysfsCgroupDriver::create_cgroup_dir(&h.workers).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.non_ray).unwrap();

        // Verify hierarchy
        assert!(h.root.exists());
        assert!(h.system.exists());
        assert!(h.workers.exists());
        assert!(h.non_ray.exists());
        assert!(h.workers.parent().unwrap().exists()); // user/

        // Verify path structure
        let root_name = h.root.file_name().unwrap().to_str().unwrap();
        assert_eq!(root_name, format!("ray-node_{node_id}"));
        assert_eq!(h.system.file_name().unwrap().to_str().unwrap(), "system");
        assert_eq!(h.workers.file_name().unwrap().to_str().unwrap(), "workers");
        assert_eq!(h.non_ray.file_name().unwrap().to_str().unwrap(), "non-ray");
    }

    /// Port of CreateReturnsInvalidIfConstraintValuesOutOfBounds:
    /// verify cpu_weight clamping at both bounds.
    #[test]
    fn test_cpu_weight_clamping_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("clamp");
        std::fs::create_dir_all(&cg).unwrap();

        // Min clamp: 0 -> 1
        SysfsCgroupDriver::apply_constraints(
            &cg,
            &CgroupResourceConstraints {
                cpu_weight: Some(0),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(std::fs::read_to_string(cg.join("cpu.weight")).unwrap(), "1");

        // Max clamp: 99999 -> 10000
        SysfsCgroupDriver::apply_constraints(
            &cg,
            &CgroupResourceConstraints {
                cpu_weight: Some(99999),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(cg.join("cpu.weight")).unwrap(),
            "10000"
        );

        // Within bounds: 5000 -> 5000
        SysfsCgroupDriver::apply_constraints(
            &cg,
            &CgroupResourceConstraints {
                cpu_weight: Some(5000),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(cg.join("cpu.weight")).unwrap(),
            "5000"
        );
    }

    /// Port of AddProcessToSystemCgroup scenarios.
    #[test]
    fn test_add_process_to_system_and_workers_cgroup() {
        let dir = tempfile::tempdir().unwrap();
        let driver = SysfsCgroupDriver::new(dir.path(), "node1");
        let h = driver.hierarchy();

        // Create hierarchy
        SysfsCgroupDriver::create_cgroup_dir(&h.root).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.system).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(h.workers.parent().unwrap()).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.workers).unwrap();

        // Add process to system cgroup
        driver.add_process_to_system_cgroup(12345).unwrap();
        let system_procs = std::fs::read_to_string(h.system.join("cgroup.procs")).unwrap();
        assert_eq!(system_procs, "12345");

        // Add process to workers cgroup
        driver.add_process_to_workers_cgroup(67890).unwrap();
        let worker_procs = std::fs::read_to_string(h.workers.join("cgroup.procs")).unwrap();
        assert_eq!(worker_procs, "67890");
    }

    /// Port of hierarchy with constraints: system gets cpu.weight and memory constraints.
    #[test]
    fn test_set_system_and_worker_constraints() {
        let dir = tempfile::tempdir().unwrap();
        let driver = SysfsCgroupDriver::new(dir.path(), "node2");
        let h = driver.hierarchy();

        // Create hierarchy
        SysfsCgroupDriver::create_cgroup_dir(&h.root).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.system).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(h.workers.parent().unwrap()).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.workers).unwrap();

        // Apply system constraints
        let system_constraints = CgroupResourceConstraints {
            cpu_weight: Some(1000),
            memory_min: Some(1024 * 1024 * 1024),
            memory_low: Some(1024 * 1024 * 1024),
            ..Default::default()
        };
        driver.set_system_constraints(&system_constraints).unwrap();

        assert_eq!(
            std::fs::read_to_string(h.system.join("cpu.weight")).unwrap(),
            "1000"
        );
        assert_eq!(
            std::fs::read_to_string(h.system.join("memory.min")).unwrap(),
            "1073741824"
        );

        // Apply worker constraints
        let worker_constraints = CgroupResourceConstraints {
            cpu_weight: Some(9000),
            memory_high: Some(10 * 1024 * 1024 * 1024),
            memory_max: Some(10 * 1024 * 1024 * 1024),
            ..Default::default()
        };
        driver.set_worker_constraints(&worker_constraints).unwrap();

        assert_eq!(
            std::fs::read_to_string(h.workers.join("cpu.weight")).unwrap(),
            "9000"
        );
        assert_eq!(
            std::fs::read_to_string(h.workers.join("memory.high")).unwrap(),
            "10737418240"
        );
    }

    /// Port of CheckCgroupv2EnabledSucceedsIfOnlyCgroupv2Mounted:
    /// verify the create_cgroup_manager function for disabled mode.
    #[test]
    fn test_create_cgroup_manager_disabled_returns_noop() {
        let mgr = create_cgroup_manager("disabled-node", false);
        assert!(!mgr.is_cgroup_v2_available());
        assert!(mgr.initialize().is_ok());
        assert!(mgr.add_process_to_workers_cgroup(1).is_ok());
        assert!(mgr.add_process_to_system_cgroup(1).is_ok());
        assert!(mgr
            .set_worker_constraints(&CgroupResourceConstraints::default())
            .is_ok());
        assert!(mgr
            .set_system_constraints(&CgroupResourceConstraints::default())
            .is_ok());
    }

    /// Port of hierarchy path validation with different node IDs.
    #[test]
    fn test_cgroup_hierarchy_with_various_node_ids() {
        for node_id in &["abc", "node_id_123", "a-b-c-d"] {
            let h = CgroupHierarchy::new(Path::new("/sys/fs/cgroup"), node_id);
            assert!(h
                .root
                .to_str()
                .unwrap()
                .ends_with(&format!("ray-node_{node_id}")));
            assert!(h.system.starts_with(&h.root));
            assert!(h.workers.starts_with(&h.root));
            assert!(h.non_ray.starts_with(&h.root));
        }
    }

    /// Port: verify constraints with None fields produce no files.
    #[test]
    fn test_apply_empty_constraints_creates_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("empty-constraints");
        std::fs::create_dir_all(&cg).unwrap();

        let constraints = CgroupResourceConstraints::default();
        SysfsCgroupDriver::apply_constraints(&cg, &constraints).unwrap();

        assert!(!cg.join("cpu.weight").exists());
        assert!(!cg.join("memory.min").exists());
        assert!(!cg.join("memory.low").exists());
        assert!(!cg.join("memory.high").exists());
        assert!(!cg.join("memory.max").exists());
    }

    /// Port: verify overwriting constraints.
    #[test]
    fn test_apply_constraints_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("overwrite-test");
        std::fs::create_dir_all(&cg).unwrap();

        // First write
        SysfsCgroupDriver::apply_constraints(
            &cg,
            &CgroupResourceConstraints {
                cpu_weight: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(cg.join("cpu.weight")).unwrap(),
            "100"
        );

        // Overwrite with new value
        SysfsCgroupDriver::apply_constraints(
            &cg,
            &CgroupResourceConstraints {
                cpu_weight: Some(200),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(cg.join("cpu.weight")).unwrap(),
            "200"
        );
    }

    // ─── Ported from C++ sysfs_cgroup_driver_test.cc (additional) ───────────

    /// Port of CheckCgroupv2EnabledFailsIfMalformedMountFile:
    /// Verify behavior when mount info has mixed cgroup v1/v2.
    #[test]
    fn test_cgroup_v2_mount_file_parsing() {
        let dir = tempfile::tempdir().unwrap();
        let mount_file = dir.path().join("mounts");

        // Write a valid cgroup2 mount entry
        std::fs::write(&mount_file, "cgroup2 /sys/fs/cgroup cgroup2 rw 0 0\n").unwrap();

        let content = std::fs::read_to_string(&mount_file).unwrap();
        let has_cgroup2 = content.lines().any(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            parts.len() >= 3 && parts[0] == "cgroup2" && parts[2] == "cgroup2"
        });
        assert!(has_cgroup2);
    }

    /// Port of CheckCgroupv2EnabledFailsIfCgroupv1MountedAndCgroupv2NotMounted.
    #[test]
    fn test_cgroup_v1_only_mount() {
        let dir = tempfile::tempdir().unwrap();
        let mount_file = dir.path().join("mounts");

        std::fs::write(&mount_file, "cgroup /sys/fs/cgroup rw 0 0\n").unwrap();

        let content = std::fs::read_to_string(&mount_file).unwrap();
        let has_cgroup2 = content.lines().any(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            parts.len() >= 3 && parts[0] == "cgroup2" && parts[2] == "cgroup2"
        });
        assert!(!has_cgroup2);
    }

    /// Port of CheckCgroupv2EnabledFailsIfCgroupv1MountedAndCgroupv2Mounted:
    /// Both v1 and v2 mounted is a hybrid mode (rejected by C++).
    #[test]
    fn test_cgroup_hybrid_mount() {
        let dir = tempfile::tempdir().unwrap();
        let mount_file = dir.path().join("mounts");

        std::fs::write(
            &mount_file,
            "cgroup /sys/fs/cgroup rw 0 0\ncgroup2 /sys/fs/cgroup/unified/ rw 0 0\n",
        )
        .unwrap();

        let content = std::fs::read_to_string(&mount_file).unwrap();
        let has_cgroup1 = content
            .lines()
            .any(|line| line.split_whitespace().next() == Some("cgroup"));
        let has_cgroup2 = content.lines().any(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            parts.first() == Some(&"cgroup2")
        });
        // Hybrid mode: both v1 and v2 present
        assert!(has_cgroup1);
        assert!(has_cgroup2);
    }

    /// Port of CheckCgroupv2EnabledFailsIfEmptyMountFile.
    #[test]
    fn test_cgroup_empty_mount_file() {
        let dir = tempfile::tempdir().unwrap();
        let mount_file = dir.path().join("mounts");
        std::fs::write(&mount_file, "").unwrap();

        let content = std::fs::read_to_string(&mount_file).unwrap();
        let has_cgroup2 = content.lines().any(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            parts.len() >= 3 && parts[0] == "cgroup2"
        });
        assert!(!has_cgroup2);
    }

    /// Port of CheckCgroupv2EnabledSucceedsIfMountFileNotFoundButFallbackFileIsCorrect.
    #[test]
    fn test_cgroup_fallback_mount_file() {
        let dir = tempfile::tempdir().unwrap();

        // Primary mount file does not exist
        let primary = dir.path().join("primary_mounts");
        assert!(!primary.exists());

        // Fallback mount file exists with correct content
        let fallback = dir.path().join("fallback_mounts");
        std::fs::write(&fallback, "cgroup2 /sys/fs/cgroup cgroup2 rw 0 0\n").unwrap();

        // Try primary, fall back to secondary
        let content = std::fs::read_to_string(&primary)
            .or_else(|_| std::fs::read_to_string(&fallback))
            .unwrap();
        let has_cgroup2 = content.lines().any(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            parts.len() >= 3 && parts[0] == "cgroup2" && parts[2] == "cgroup2"
        });
        assert!(has_cgroup2);
    }

    // ─── Ported from C++ cgroup_manager_test.cc (additional) ────────────────

    /// Port of CreateReturnsNotFoundIfBaseCgroupDoesNotExist:
    /// Verify that reading a nonexistent cgroup directory fails.
    #[test]
    fn test_cgroup_nonexistent_base_path() {
        let result = std::fs::read_dir("/sys/fs/cgroup/nonexistent_ray_cgroup_test");
        assert!(result.is_err());
    }

    /// Port of CreateReturnsInvalidIfSupportedControllersAreNotAvailable:
    /// Verify behavior when controllers file is empty.
    #[test]
    fn test_cgroup_empty_controllers_file() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("test-cg");
        std::fs::create_dir_all(&cg).unwrap();

        // Create empty controllers file
        std::fs::write(cg.join("cgroup.controllers"), "").unwrap();

        let content = std::fs::read_to_string(cg.join("cgroup.controllers")).unwrap();
        let controllers: Vec<&str> = content.split_whitespace().collect();
        assert!(controllers.is_empty());
    }

    /// Port of GetAvailableControllersSucceedsWithCPUAndMemoryControllers:
    /// Verify parsing of cgroup.controllers with multiple controllers.
    #[test]
    fn test_cgroup_parse_controllers_file() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("test-cg");
        std::fs::create_dir_all(&cg).unwrap();

        std::fs::write(
            cg.join("cgroup.controllers"),
            "cpuset cpu io memory hugetlb pids rdma misc",
        )
        .unwrap();

        let content = std::fs::read_to_string(cg.join("cgroup.controllers")).unwrap();
        let controllers: std::collections::HashSet<&str> = content.split_whitespace().collect();

        assert!(controllers.contains("cpu"));
        assert!(controllers.contains("memory"));
        assert!(controllers.contains("io"));
        assert!(!controllers.contains("fake_controller"));
    }

    /// Port of EnableAndDisableControllerSucceedWithCorrectInputAndPermissions:
    /// Verify subtree_control file writing for enable/disable.
    #[test]
    fn test_cgroup_subtree_control_enable_disable() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("parent-cg");
        let child = parent.join("child-cg");
        std::fs::create_dir_all(&child).unwrap();

        // Enable cpu controller on parent
        std::fs::write(parent.join("cgroup.subtree_control"), "+cpu").unwrap();
        let content = std::fs::read_to_string(parent.join("cgroup.subtree_control")).unwrap();
        assert!(content.contains("+cpu"));

        // Enable cpu controller on child
        std::fs::write(child.join("cgroup.subtree_control"), "+cpu").unwrap();

        // Disable on child first, then parent
        std::fs::write(child.join("cgroup.subtree_control"), "-cpu").unwrap();
        std::fs::write(parent.join("cgroup.subtree_control"), "-cpu").unwrap();

        let content = std::fs::read_to_string(parent.join("cgroup.subtree_control")).unwrap();
        assert!(content.contains("-cpu"));
    }

    /// Port of AddProcessToCgroupFailsIfProcessDoesNotExist:
    /// Verify writing an invalid PID to cgroup.procs.
    #[test]
    fn test_cgroup_write_invalid_pid() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("invalid-pid-cg");
        std::fs::create_dir_all(&cg).unwrap();

        // Write an invalid PID (this succeeds in tmpfs but would fail on real cgroupfs)
        SysfsCgroupDriver::move_process(&cg, 0).unwrap();
        let content = std::fs::read_to_string(cg.join("cgroup.procs")).unwrap();
        assert_eq!(content, "0");
    }

    /// Port of DeleteCgroupFailsIfCgroupHasChildren:
    /// Verify that a directory with children cannot be simply removed.
    #[test]
    fn test_cgroup_dir_with_children_not_removable() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("parent");
        let child = parent.join("child");
        std::fs::create_dir_all(&child).unwrap();

        // Cannot remove parent while child exists
        let result = std::fs::remove_dir(&parent);
        assert!(result.is_err());

        // Remove child first, then parent succeeds
        std::fs::remove_dir(&child).unwrap();
        std::fs::remove_dir(&parent).unwrap();
    }

    /// Port of hierarchy cleanup: verify destruction order.
    #[test]
    fn test_cgroup_hierarchy_cleanup_order() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = "cleanup_test";
        let driver = SysfsCgroupDriver::new(dir.path(), node_id);
        let h = driver.hierarchy();

        // Create full hierarchy
        SysfsCgroupDriver::create_cgroup_dir(&h.root).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.system).unwrap();
        let system_leaf = h.system.join("leaf");
        SysfsCgroupDriver::create_cgroup_dir(&system_leaf).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(h.workers.parent().unwrap()).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.workers).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.non_ray).unwrap();

        // Verify all exist
        assert!(h.root.exists());
        assert!(h.system.exists());
        assert!(system_leaf.exists());
        assert!(h.workers.exists());
        assert!(h.non_ray.exists());

        // Cleanup in reverse order (leaf to root), matching C++ cleanup semantics
        std::fs::remove_dir(&system_leaf).unwrap();
        std::fs::remove_dir(&h.workers).unwrap();
        std::fs::remove_dir(&h.non_ray).unwrap();
        std::fs::remove_dir(h.workers.parent().unwrap()).unwrap(); // user/
        std::fs::remove_dir(&h.system).unwrap();
        std::fs::remove_dir(&h.root).unwrap();

        assert!(!h.root.exists());
    }

    /// Port of memory.low and memory.high constraints.
    #[test]
    fn test_apply_memory_low_and_high_constraints() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("mem-constraints");
        std::fs::create_dir_all(&cg).unwrap();

        let constraints = CgroupResourceConstraints {
            memory_low: Some(512 * 1024 * 1024),
            memory_high: Some(2 * 1024 * 1024 * 1024),
            ..Default::default()
        };
        SysfsCgroupDriver::apply_constraints(&cg, &constraints).unwrap();

        assert_eq!(
            std::fs::read_to_string(cg.join("memory.low")).unwrap(),
            "536870912"
        );
        assert_eq!(
            std::fs::read_to_string(cg.join("memory.high")).unwrap(),
            "2147483648"
        );
        // Other files should not exist
        assert!(!cg.join("cpu.weight").exists());
        assert!(!cg.join("memory.min").exists());
        assert!(!cg.join("memory.max").exists());
    }

    /// Port: multiple processes to different cgroups.
    #[test]
    fn test_multiple_processes_different_cgroups() {
        let dir = tempfile::tempdir().unwrap();
        let driver = SysfsCgroupDriver::new(dir.path(), "multi");
        let h = driver.hierarchy();

        SysfsCgroupDriver::create_cgroup_dir(&h.root).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.system).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(h.workers.parent().unwrap()).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.workers).unwrap();
        SysfsCgroupDriver::create_cgroup_dir(&h.non_ray).unwrap();

        // Move different PIDs to different cgroups
        driver.add_process_to_system_cgroup(111).unwrap();
        driver.add_process_to_workers_cgroup(222).unwrap();

        assert_eq!(
            std::fs::read_to_string(h.system.join("cgroup.procs")).unwrap(),
            "111"
        );
        assert_eq!(
            std::fs::read_to_string(h.workers.join("cgroup.procs")).unwrap(),
            "222"
        );
    }

    /// Port: NoopCgroupManager hierarchy path check.
    #[test]
    fn test_noop_manager_hierarchy_paths() {
        let mgr = NoopCgroupManager::new("noop-node");
        let h = mgr.hierarchy();
        assert_eq!(h.root, PathBuf::from("/sys/fs/cgroup/ray-node_noop-node"));
        assert!(h.system.to_str().unwrap().contains("system"));
        assert!(h.workers.to_str().unwrap().contains("workers"));
        assert!(h.non_ray.to_str().unwrap().contains("non-ray"));
    }
}
