// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! File system utilities for Ray.
//!
//! Provides temp directory management, log rotation, and socket path generation.
//!
//! Replaces `src/ray/util/filesystem.cc`.

use std::io;
use std::path::{Path, PathBuf};

/// Default Ray temp directory base (can be overridden by `RAY_TMPDIR` env var).
pub const RAY_TEMP_DIR_BASE: &str = "/tmp/ray";

/// Environment variable to override the Ray temp directory base.
/// Matches `RAY_TMPDIR_ENV` in `ray-common/constants.rs`.
const RAY_TMPDIR_ENV: &str = "RAY_TMPDIR";

/// Get the Ray temp directory base, respecting the `RAY_TMPDIR` environment variable.
fn ray_temp_dir_base() -> PathBuf {
    std::env::var(RAY_TMPDIR_ENV)
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(RAY_TEMP_DIR_BASE))
}

/// Get the Ray temp directory for the current session.
///
/// If `session_name` is provided, returns `<base>/<session_name>/`.
/// Otherwise returns `<base>/`.
///
/// The base directory defaults to `/tmp/ray` but can be overridden via
/// the `RAY_TMPDIR` environment variable.
pub fn get_ray_temp_dir(session_name: Option<&str>) -> PathBuf {
    let base = ray_temp_dir_base();
    match session_name {
        Some(name) => base.join(name),
        None => base,
    }
}

/// Create a temporary directory under the Ray temp directory.
///
/// Returns the path to the created directory.
pub fn create_ray_temp_dir(session_name: Option<&str>, prefix: &str) -> io::Result<PathBuf> {
    let base = get_ray_temp_dir(session_name);
    std::fs::create_dir_all(&base)?;
    let random_suffix: String = crate::random::random_bytes(8)
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    let dir_name = format!("{}{}", prefix, random_suffix);
    let dir_path = base.join(dir_name);
    std::fs::create_dir_all(&dir_path)?;
    Ok(dir_path)
}

/// Generate a socket path for a Ray component.
///
/// Returns a path like `/tmp/ray/<session>/sockets/<component>`.
pub fn get_socket_path(session_name: &str, component: &str) -> PathBuf {
    let base = get_ray_temp_dir(Some(session_name));
    let sockets_dir = base.join("sockets");
    sockets_dir.join(component)
}

/// Get the log directory for a Ray session.
pub fn get_log_dir(session_name: &str) -> PathBuf {
    get_ray_temp_dir(Some(session_name)).join("logs")
}

/// Ensure a directory exists, creating it if necessary.
pub fn ensure_dir_exists(path: &Path) -> io::Result<()> {
    if !path.exists() {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}

/// Rotate a log file if it exceeds `max_size` bytes.
///
/// Renames `path` to `path.1`, `path.1` to `path.2`, etc., up to `max_files`.
/// Older files beyond `max_files` are deleted.
pub fn rotate_log_file(path: &Path, max_size: u64, max_files: u32) -> io::Result<bool> {
    // Check if the file exists and exceeds max size.
    let metadata = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };

    if metadata.len() <= max_size {
        return Ok(false);
    }

    // Shift existing rotated files: .N → .N+1 (starting from max).
    for i in (1..max_files).rev() {
        let from = rotated_path(path, i);
        let to = rotated_path(path, i + 1);
        if from.exists() {
            if i + 1 > max_files {
                std::fs::remove_file(&from)?;
            } else {
                std::fs::rename(&from, &to)?;
            }
        }
    }

    // Delete the oldest file if at limit.
    let oldest = rotated_path(path, max_files);
    if oldest.exists() {
        std::fs::remove_file(&oldest)?;
    }

    // Rotate current file to .1.
    let target = rotated_path(path, 1);
    std::fs::rename(path, &target)?;

    Ok(true)
}

/// Build a rotated file path: `path.N`.
fn rotated_path(path: &Path, index: u32) -> PathBuf {
    let mut rotated = path.to_path_buf().into_os_string();
    rotated.push(format!(".{}", index));
    PathBuf::from(rotated)
}

/// Recursively remove a directory and all its contents.
/// Returns the number of entries removed.
pub fn remove_dir_recursive(path: &Path) -> io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            count += remove_dir_recursive(&path)?;
        } else {
            std::fs::remove_file(&path)?;
            count += 1;
        }
    }
    std::fs::remove_dir(path)?;
    count += 1;
    Ok(count)
}

/// Get total size of a directory in bytes.
pub fn dir_size(path: &Path) -> io::Result<u64> {
    let mut total = 0;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                total += dir_size(&path)?;
            } else {
                total += std::fs::metadata(&path)?.len();
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_ray_temp_dir() {
        let dir = get_ray_temp_dir(None);
        assert_eq!(dir, PathBuf::from("/tmp/ray"));

        let dir = get_ray_temp_dir(Some("session_123"));
        assert_eq!(dir, PathBuf::from("/tmp/ray/session_123"));
    }

    #[test]
    fn test_get_socket_path() {
        let path = get_socket_path("test_session", "raylet");
        assert_eq!(
            path,
            PathBuf::from("/tmp/ray/test_session/sockets/raylet")
        );
    }

    #[test]
    fn test_get_log_dir() {
        let dir = get_log_dir("test_session");
        assert_eq!(dir, PathBuf::from("/tmp/ray/test_session/logs"));
    }

    #[test]
    fn test_create_ray_temp_dir() {
        let dir = create_ray_temp_dir(Some("test_fs_utils"), "test_").unwrap();
        assert!(dir.exists());
        // On macOS, /tmp may resolve to /private/tmp, so check the filename part.
        let dir_name = dir.file_name().unwrap().to_str().unwrap();
        assert!(dir_name.starts_with("test_"), "dir name should start with prefix: {dir_name}");
        // Cleanup.
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_ensure_dir_exists() {
        let base = PathBuf::from("/tmp/ray/test_ensure_dir");
        let nested = base.join("a").join("b").join("c");
        ensure_dir_exists(&nested).unwrap();
        assert!(nested.exists());
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn test_rotate_log_file() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Create a file that exceeds the max size.
        std::fs::write(&log_path, vec![b'a'; 200]).unwrap();

        // Rotate with max_size=100 (file is 200, so should rotate).
        let rotated = rotate_log_file(&log_path, 100, 3).unwrap();
        assert!(rotated);
        assert!(!log_path.exists());
        assert!(rotated_path(&log_path, 1).exists());

        // Create new file and rotate again.
        std::fs::write(&log_path, vec![b'b'; 200]).unwrap();
        rotate_log_file(&log_path, 100, 3).unwrap();
        assert!(rotated_path(&log_path, 1).exists());
        assert!(rotated_path(&log_path, 2).exists());
    }

    #[test]
    fn test_rotate_no_rotation_needed() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        std::fs::write(&log_path, vec![b'a'; 50]).unwrap();
        let rotated = rotate_log_file(&log_path, 100, 3).unwrap();
        assert!(!rotated);
        assert!(log_path.exists());
    }

    #[test]
    fn test_rotate_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("nonexistent.log");
        let rotated = rotate_log_file(&log_path, 100, 3).unwrap();
        assert!(!rotated);
    }

    #[test]
    fn test_rotated_path() {
        let path = Path::new("/tmp/test.log");
        assert_eq!(rotated_path(path, 1), PathBuf::from("/tmp/test.log.1"));
        assert_eq!(rotated_path(path, 3), PathBuf::from("/tmp/test.log.3"));
    }

    #[test]
    fn test_dir_size() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), vec![0u8; 100]).unwrap();
        std::fs::write(dir.path().join("b.txt"), vec![0u8; 200]).unwrap();
        let size = dir_size(dir.path()).unwrap();
        assert_eq!(size, 300);
    }

    #[test]
    fn test_remove_dir_recursive() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("file.txt"), b"data").unwrap();
        std::fs::write(dir.path().join("root.txt"), b"root").unwrap();

        let count = remove_dir_recursive(dir.path()).unwrap();
        assert!(count >= 3); // sub, file.txt, root.txt, dir itself
        assert!(!dir.path().exists());
    }
}
