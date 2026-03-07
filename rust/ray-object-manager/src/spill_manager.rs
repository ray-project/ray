// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object spill/restore manager.
//!
//! Handles spilling objects to local disk (or external storage) when the
//! object store exceeds capacity, and restoring them on demand.
//!
//! Spill URL format: `file:///path/to/spilled?offset=N&size=M`

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use ray_common::id::ObjectID;

/// Configuration for the spill manager.
#[derive(Debug, Clone)]
pub struct SpillManagerConfig {
    /// Directory to spill objects to.
    pub spill_directory: PathBuf,
    /// Maximum number of bytes to spill in a single batch.
    pub max_spill_batch_bytes: u64,
    /// Maximum number of objects to spill in a single batch.
    pub max_spill_batch_count: usize,
}

impl Default for SpillManagerConfig {
    fn default() -> Self {
        Self {
            spill_directory: PathBuf::from("/tmp/ray_spilled_objects"),
            max_spill_batch_bytes: 100 * 1024 * 1024, // 100MB
            max_spill_batch_count: 100,
        }
    }
}

/// Information about a spilled object.
#[derive(Debug, Clone)]
pub struct SpilledObjectInfo {
    pub object_id: ObjectID,
    /// URL to the spilled data, e.g. `file:///path?offset=N&size=M`.
    pub url: String,
    /// Size of the spilled data.
    pub data_size: u64,
    /// Size of the metadata.
    pub metadata_size: u64,
}

/// The spill file header: [address_size:8][metadata_size:8][data_size:8].
const SPILL_HEADER_SIZE: usize = 24;

/// Inner state protected by a mutex.
struct SpillManagerInner {
    /// Mapping from object_id → spill URL.
    spilled_objects: HashMap<ObjectID, SpilledObjectInfo>,
    /// Current spill file index (for naming).
    next_file_index: u64,
}

/// Manages spilling and restoring objects from external storage.
pub struct SpillManager {
    config: SpillManagerConfig,
    inner: Mutex<SpillManagerInner>,
    total_bytes_spilled: AtomicU64,
    total_bytes_restored: AtomicU64,
    total_objects_spilled: AtomicU64,
    total_objects_restored: AtomicU64,
}

impl SpillManager {
    /// Create a new spill manager with the given config.
    pub fn new(config: SpillManagerConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(SpillManagerInner {
                spilled_objects: HashMap::new(),
                next_file_index: 0,
            }),
            total_bytes_spilled: AtomicU64::new(0),
            total_bytes_restored: AtomicU64::new(0),
            total_objects_spilled: AtomicU64::new(0),
            total_objects_restored: AtomicU64::new(0),
        }
    }

    /// Spill an object to disk.
    ///
    /// Writes the object data to a file in the spill directory.
    /// Returns the spill URL on success.
    pub fn spill_object(
        &self,
        object_id: &ObjectID,
        data: &[u8],
        metadata: &[u8],
    ) -> Result<String, SpillError> {
        let file_index = {
            let mut inner = self.inner.lock();
            let idx = inner.next_file_index;
            inner.next_file_index += 1;
            idx
        };

        // Ensure spill directory exists.
        std::fs::create_dir_all(&self.config.spill_directory)
            .map_err(|e| SpillError::IoError(e.to_string()))?;

        let file_name = format!(
            "object_{:016x}_{}",
            file_index,
            hex::encode(&object_id.binary())
        );
        let file_path = self.config.spill_directory.join(&file_name);

        // Write in spill format: [addr_size:8][meta_size:8][data_size:8][addr][meta][data]
        let address = object_id.binary();
        let addr_size = (address.len() as u64).to_le_bytes();
        let meta_size = (metadata.len() as u64).to_le_bytes();
        let data_size = (data.len() as u64).to_le_bytes();

        let mut contents =
            Vec::with_capacity(SPILL_HEADER_SIZE + address.len() + metadata.len() + data.len());
        contents.extend_from_slice(&addr_size);
        contents.extend_from_slice(&meta_size);
        contents.extend_from_slice(&data_size);
        contents.extend_from_slice(&address);
        contents.extend_from_slice(metadata);
        contents.extend_from_slice(data);

        std::fs::write(&file_path, &contents).map_err(|e| SpillError::IoError(e.to_string()))?;

        let url = format!(
            "file://{}?offset=0&size={}",
            file_path.display(),
            contents.len()
        );

        let info = SpilledObjectInfo {
            object_id: *object_id,
            url: url.clone(),
            data_size: data.len() as u64,
            metadata_size: metadata.len() as u64,
        };

        let mut inner = self.inner.lock();
        inner.spilled_objects.insert(*object_id, info);

        self.total_bytes_spilled
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.total_objects_spilled.fetch_add(1, Ordering::Relaxed);

        Ok(url)
    }

    /// Restore an object from a spill URL.
    ///
    /// Reads the object data back from disk.
    /// Returns (data, metadata) on success.
    pub fn restore_object(&self, url: &str) -> Result<(Vec<u8>, Vec<u8>), SpillError> {
        let (file_path, offset, size) = parse_spill_url(url)?;

        let file_contents = std::fs::read(&file_path)
            .map_err(|e| SpillError::IoError(format!("{}: {}", file_path, e)))?;

        let region = if size > 0 {
            if offset + size > file_contents.len() {
                return Err(SpillError::IoError(
                    "Spill region out of bounds".to_string(),
                ));
            }
            &file_contents[offset..offset + size]
        } else {
            &file_contents[offset..]
        };

        if region.len() < SPILL_HEADER_SIZE {
            return Err(SpillError::CorruptData(
                "Spill header too small".to_string(),
            ));
        }

        let addr_size = u64::from_le_bytes(region[0..8].try_into().unwrap()) as usize;
        let meta_size = u64::from_le_bytes(region[8..16].try_into().unwrap()) as usize;
        let data_size = u64::from_le_bytes(region[16..24].try_into().unwrap()) as usize;

        let expected_total = SPILL_HEADER_SIZE + addr_size + meta_size + data_size;
        if region.len() < expected_total {
            return Err(SpillError::CorruptData(format!(
                "Spill data truncated: expected {} bytes, got {}",
                expected_total,
                region.len()
            )));
        }

        let meta_start = SPILL_HEADER_SIZE + addr_size;
        let data_start = meta_start + meta_size;

        let metadata = region[meta_start..data_start].to_vec();
        let data = region[data_start..data_start + data_size].to_vec();

        self.total_bytes_restored
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.total_objects_restored.fetch_add(1, Ordering::Relaxed);

        Ok((data, metadata))
    }

    /// Delete a spilled object from disk.
    pub fn delete_spilled_object(&self, url: &str) -> Result<(), SpillError> {
        let (file_path, _, _) = parse_spill_url(url)?;
        if Path::new(&file_path).exists() {
            std::fs::remove_file(&file_path).map_err(|e| SpillError::IoError(e.to_string()))?;
        }
        Ok(())
    }

    /// Get the spill URL for an object (if it was spilled by this manager).
    pub fn get_spill_url(&self, object_id: &ObjectID) -> Option<String> {
        self.inner
            .lock()
            .spilled_objects
            .get(object_id)
            .map(|info| info.url.clone())
    }

    /// Remove tracking of a spilled object (without deleting the file).
    pub fn untrack_object(&self, object_id: &ObjectID) {
        self.inner.lock().spilled_objects.remove(object_id);
    }

    /// Number of objects currently tracked as spilled.
    pub fn num_spilled_objects(&self) -> usize {
        self.inner.lock().spilled_objects.len()
    }

    /// Total bytes spilled since creation.
    pub fn total_bytes_spilled(&self) -> u64 {
        self.total_bytes_spilled.load(Ordering::Relaxed)
    }

    /// Total bytes restored since creation.
    pub fn total_bytes_restored(&self) -> u64 {
        self.total_bytes_restored.load(Ordering::Relaxed)
    }

    /// Total objects spilled since creation.
    pub fn total_objects_spilled(&self) -> u64 {
        self.total_objects_spilled.load(Ordering::Relaxed)
    }

    /// Total objects restored since creation.
    pub fn total_objects_restored(&self) -> u64 {
        self.total_objects_restored.load(Ordering::Relaxed)
    }
}

/// Parse a spill URL into (file_path, offset, size).
fn parse_spill_url(url: &str) -> Result<(String, usize, usize), SpillError> {
    let url = url.strip_prefix("file://").unwrap_or(url);

    let (path, query) = match url.split_once('?') {
        Some((p, q)) => (p, q),
        None => return Ok((url.to_string(), 0, 0)),
    };

    let mut offset = 0usize;
    let mut size = 0usize;

    for param in query.split('&') {
        if let Some((key, value)) = param.split_once('=') {
            match key {
                "offset" => {
                    offset = value
                        .parse()
                        .map_err(|_| SpillError::InvalidUrl(url.to_string()))?;
                }
                "size" => {
                    size = value
                        .parse()
                        .map_err(|_| SpillError::InvalidUrl(url.to_string()))?;
                }
                _ => {}
            }
        }
    }

    Ok((path.to_string(), offset, size))
}

/// Errors from spill/restore operations.
#[derive(Debug, thiserror::Error)]
pub enum SpillError {
    #[error("I/O error: {0}")]
    IoError(String),
    #[error("Corrupt spill data: {0}")]
    CorruptData(String),
    #[error("Invalid spill URL: {0}")]
    InvalidUrl(String),
}

// hex encoding helper (minimal, no external dep needed if not available)
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    fn make_temp_config() -> SpillManagerConfig {
        let dir = std::env::temp_dir().join(format!(
            "ray_spill_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        SpillManagerConfig {
            spill_directory: dir,
            ..Default::default()
        }
    }

    fn cleanup(config: &SpillManagerConfig) {
        let _ = fs::remove_dir_all(&config.spill_directory);
    }

    #[test]
    fn test_spill_and_restore() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());
        let oid = make_oid(1);

        let data = b"hello world";
        let metadata = b"meta";

        let url = mgr.spill_object(&oid, data, metadata).unwrap();
        assert!(url.starts_with("file://"));

        let (restored_data, restored_meta) = mgr.restore_object(&url).unwrap();
        assert_eq!(restored_data, data);
        assert_eq!(restored_meta, metadata);

        assert_eq!(mgr.total_objects_spilled(), 1);
        assert_eq!(mgr.total_bytes_spilled(), data.len() as u64);
        assert_eq!(mgr.total_objects_restored(), 1);

        cleanup(&config);
    }

    #[test]
    fn test_spill_creates_directory() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());

        assert!(!config.spill_directory.exists());
        mgr.spill_object(&make_oid(1), b"data", b"").unwrap();
        assert!(config.spill_directory.exists());

        cleanup(&config);
    }

    #[test]
    fn test_get_spill_url() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());
        let oid = make_oid(1);

        assert!(mgr.get_spill_url(&oid).is_none());

        mgr.spill_object(&oid, b"data", b"").unwrap();
        assert!(mgr.get_spill_url(&oid).is_some());

        cleanup(&config);
    }

    #[test]
    fn test_delete_spilled_object() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());
        let oid = make_oid(1);

        let url = mgr.spill_object(&oid, b"data", b"meta").unwrap();
        let (path, _, _) = parse_spill_url(&url).unwrap();
        assert!(Path::new(&path).exists());

        mgr.delete_spilled_object(&url).unwrap();
        assert!(!Path::new(&path).exists());

        cleanup(&config);
    }

    #[test]
    fn test_untrack_object() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());
        let oid = make_oid(1);

        mgr.spill_object(&oid, b"data", b"").unwrap();
        assert_eq!(mgr.num_spilled_objects(), 1);

        mgr.untrack_object(&oid);
        assert_eq!(mgr.num_spilled_objects(), 0);

        cleanup(&config);
    }

    #[test]
    fn test_multiple_spills() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());

        for i in 0..5u8 {
            mgr.spill_object(&make_oid(i), &[i; 100], &[i; 10]).unwrap();
        }

        assert_eq!(mgr.num_spilled_objects(), 5);
        assert_eq!(mgr.total_objects_spilled(), 5);
        assert_eq!(mgr.total_bytes_spilled(), 500); // 5 * 100

        // Restore each one.
        for i in 0..5u8 {
            let url = mgr.get_spill_url(&make_oid(i)).unwrap();
            let (data, meta) = mgr.restore_object(&url).unwrap();
            assert_eq!(data, vec![i; 100]);
            assert_eq!(meta, vec![i; 10]);
        }

        cleanup(&config);
    }

    #[test]
    fn test_restore_nonexistent_returns_error() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());

        let result = mgr.restore_object("file:///nonexistent/path");
        assert!(result.is_err());

        cleanup(&config);
    }

    #[test]
    fn test_parse_spill_url() {
        let (path, offset, size) =
            parse_spill_url("file:///tmp/spill/obj1?offset=100&size=200").unwrap();
        assert_eq!(path, "/tmp/spill/obj1");
        assert_eq!(offset, 100);
        assert_eq!(size, 200);
    }

    #[test]
    fn test_parse_spill_url_no_query() {
        let (path, offset, size) = parse_spill_url("file:///tmp/spill/obj1").unwrap();
        assert_eq!(path, "/tmp/spill/obj1");
        assert_eq!(offset, 0);
        assert_eq!(size, 0);
    }

    #[test]
    fn test_spill_empty_data() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());
        let oid = make_oid(1);

        let url = mgr.spill_object(&oid, b"", b"").unwrap();
        let (data, meta) = mgr.restore_object(&url).unwrap();
        assert!(data.is_empty());
        assert!(meta.is_empty());

        cleanup(&config);
    }

    // --- Ported from C++ spilled_object_test.cc: ParseObjectURL ---

    /// Port of C++ ParseObjectURL: various valid URL formats.
    #[test]
    fn test_parse_spill_url_various_formats() {
        // file:// prefix with path
        let (path, offset, size) =
            parse_spill_url("file://path/to/file?offset=123&size=456").unwrap();
        assert_eq!(path, "path/to/file");
        assert_eq!(offset, 123);
        assert_eq!(size, 456);

        // http prefix (treated as path after stripping file:// if present)
        let (path, offset, size) = parse_spill_url("http://123?offset=123&size=456").unwrap();
        assert_eq!(path, "http://123");
        assert_eq!(offset, 123);
        assert_eq!(size, 456);

        // Windows-style path
        let (path, offset, size) =
            parse_spill_url("file:///C:/Users/file.txt?offset=123&size=456").unwrap();
        assert_eq!(path, "/C:/Users/file.txt");
        assert_eq!(offset, 123);
        assert_eq!(size, 456);

        // Plain unix path
        let (path, offset, size) = parse_spill_url("/tmp/file.txt?offset=123&size=456").unwrap();
        assert_eq!(path, "/tmp/file.txt");
        assert_eq!(offset, 123);
        assert_eq!(size, 456);

        // Long session path with large size
        let long_url = "/tmp/ray/session_2021-07-19_09-50-58_115365_119/ray_spillled_objects/\
            2f81e7cfcc578f4effffffffffffffffffffffff0200000001000000-multi-1?offset=0&size=2199437144";
        let (path, offset, size) = parse_spill_url(long_url).unwrap();
        assert!(path.contains("ray_spillled_objects"));
        assert_eq!(offset, 0);
        assert_eq!(size, 2199437144);

        // Large size near u64 max (9223372036854775807 = i64::MAX)
        let (path, offset, size) =
            parse_spill_url("/tmp/123?offset=0&size=9223372036854775807").unwrap();
        assert_eq!(path, "/tmp/123");
        assert_eq!(offset, 0);
        assert_eq!(size, 9223372036854775807);
    }

    /// Port of C++ ParseObjectURL: invalid URL formats that should fail.
    #[test]
    fn test_parse_spill_url_invalid_offset() {
        // Non-numeric offset
        let result = parse_spill_url("file://path/to/file?offset=a&size=456");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_spill_url_invalid_size() {
        // Non-numeric size
        let result = parse_spill_url("file://path/to/file?offset=0&size=bb");
        assert!(result.is_err());
    }

    /// Port of C++ ToUINT64: little-endian byte conversion.
    #[test]
    fn test_le_bytes_to_u64() {
        assert_eq!(
            u64::from_le_bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            0
        );
        assert_eq!(
            u64::from_le_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            1
        );
        assert_eq!(
            u64::from_le_bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            u64::MAX
        );
    }

    /// Port of C++ ReadUINT64: sequential reads from a byte buffer.
    #[test]
    fn test_read_u64_from_buffer() {
        let buf: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // u64::MAX
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // malformed (only 6 bytes)
        ];
        let v0 = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(v0, 0);
        let v1 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        assert_eq!(v1, 1);
        let v2 = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        assert_eq!(v2, u64::MAX);
        // Malformed: not enough bytes for a u64
        assert!(buf[24..].len() < 8);
    }

    /// Port of C++ ParseObjectHeader: construct a spill file and verify header parsing.
    #[test]
    fn test_spill_header_parsing() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());
        let oid = make_oid(42);

        let data = b"somedata";
        let metadata = b"somemetadata";

        let url = mgr.spill_object(&oid, data, metadata).unwrap();
        let (restored_data, restored_meta) = mgr.restore_object(&url).unwrap();
        assert_eq!(restored_data, data);
        assert_eq!(restored_meta, metadata);

        // Verify the file format directly
        let (file_path, offset, size) = parse_spill_url(&url).unwrap();
        let contents = fs::read(&file_path).unwrap();
        let region = &contents[offset..offset + size];

        // Header: [addr_size:8][meta_size:8][data_size:8]
        let addr_size = u64::from_le_bytes(region[0..8].try_into().unwrap()) as usize;
        let meta_size = u64::from_le_bytes(region[8..16].try_into().unwrap()) as usize;
        let data_size = u64::from_le_bytes(region[16..24].try_into().unwrap()) as usize;

        assert_eq!(addr_size, 28); // ObjectID is 28 bytes
        assert_eq!(meta_size, metadata.len());
        assert_eq!(data_size, data.len());

        // Verify content layout
        let meta_start = 24 + addr_size;
        let data_start = meta_start + meta_size;
        assert_eq!(&region[meta_start..data_start], metadata);
        assert_eq!(&region[data_start..data_start + data_size], data);

        cleanup(&config);
    }

    /// Port of C++ CreateSpilledObjectReader: corrupt file should fail.
    #[test]
    fn test_restore_corrupt_file() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());
        let oid = make_oid(1);

        let url = mgr.spill_object(&oid, b"data", b"meta").unwrap();
        let (file_path, _, _) = parse_spill_url(&url).unwrap();

        // Truncate the file to corrupt it
        fs::write(&file_path, b"short").unwrap();

        let result = mgr.restore_object(&url);
        assert!(result.is_err());

        cleanup(&config);
    }

    /// Port of C++ GetChunk concept: verify data/metadata can be read in chunks.
    #[test]
    fn test_spill_chunked_read() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());
        let oid = make_oid(1);

        let data = vec![0xABu8; 1000];
        let metadata = vec![0xCDu8; 200];

        let url = mgr.spill_object(&oid, &data, &metadata).unwrap();

        // Read back and verify slicing works
        let (restored_data, restored_meta) = mgr.restore_object(&url).unwrap();
        assert_eq!(restored_data.len(), 1000);
        assert_eq!(restored_meta.len(), 200);

        // Verify chunks
        let chunk_size = 100;
        for i in (0..data.len()).step_by(chunk_size) {
            let end = (i + chunk_size).min(data.len());
            assert_eq!(&restored_data[i..end], &data[i..end]);
        }

        cleanup(&config);
    }

    #[test]
    fn test_spill_large_metadata() {
        let config = make_temp_config();
        let mgr = SpillManager::new(config.clone());
        let oid = make_oid(1);

        let data = vec![0xAB; 1024];
        let metadata = vec![0xCD; 512];

        let url = mgr.spill_object(&oid, &data, &metadata).unwrap();
        let (restored_data, restored_meta) = mgr.restore_object(&url).unwrap();
        assert_eq!(restored_data, data);
        assert_eq!(restored_meta, metadata);

        cleanup(&config);
    }
}
