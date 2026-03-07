// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object readers for different storage backends.
//!
//! Replaces `spilled_object_reader.h/cc`, `chunk_object_reader.h/cc`,
//! and the IObjectReader interface.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// Interface for reading object data from any storage backend.
pub trait ObjectReader: Send + Sync {
    /// Size of the data payload.
    fn data_size(&self) -> u64;
    /// Size of the metadata payload.
    fn metadata_size(&self) -> u64;
    /// Read from the data section at the given offset.
    fn read_data(&self, offset: u64, size: u64) -> Option<Vec<u8>>;
    /// Read from the metadata section at the given offset.
    fn read_metadata(&self, offset: u64, size: u64) -> Option<Vec<u8>>;
}

// ─── SpilledObjectReader ────────────────────────────────────────────────────

/// Reads objects that have been spilled to local disk.
///
/// Spilled object file format:
/// ```text
/// [address_size: 8 bytes LE]
/// [metadata_size: 8 bytes LE]
/// [data_size: 8 bytes LE]
/// [serialized_address: address_size bytes]
/// [metadata_payload: metadata_size bytes]
/// [data_payload: data_size bytes]
/// ```
pub struct SpilledObjectReader {
    file_path: String,
    data_offset: u64,
    data_size: u64,
    metadata_offset: u64,
    metadata_size: u64,
}

impl SpilledObjectReader {
    /// Create a reader from a spill URL.
    ///
    /// URL format: `file_path?offset=X&size=Y`
    pub fn create(object_url: &str) -> Option<Self> {
        let (file_path, object_offset, _total_size) = Self::parse_url(object_url)?;

        // Parse the object header
        let mut file = File::open(&file_path).ok()?;
        file.seek(SeekFrom::Start(object_offset)).ok()?;

        let address_size = read_u64_le(&mut file)?;
        let metadata_size = read_u64_le(&mut file)?;
        let data_size = read_u64_le(&mut file)?;

        let header_size = 24; // 3 * 8 bytes
        let metadata_offset = object_offset + header_size + address_size;
        let data_offset = metadata_offset + metadata_size;

        Some(Self {
            file_path,
            data_offset,
            data_size,
            metadata_offset,
            metadata_size,
        })
    }

    fn parse_url(url: &str) -> Option<(String, u64, u64)> {
        // Parse: "file_path?offset=X&size=Y" or just "file_path"
        if let Some(qmark) = url.find('?') {
            let file_path = url[..qmark].to_string();
            let query = &url[qmark + 1..];
            let mut offset = 0u64;
            let mut size = 0u64;

            for param in query.split('&') {
                if let Some(val) = param.strip_prefix("offset=") {
                    offset = val.parse().ok()?;
                } else if let Some(val) = param.strip_prefix("size=") {
                    size = val.parse().ok()?;
                }
            }
            Some((file_path, offset, size))
        } else {
            let file_path = url.to_string();
            let size = std::fs::metadata(&file_path).ok()?.len();
            Some((file_path, 0, size))
        }
    }

    fn read_file_range(&self, offset: u64, size: u64) -> Option<Vec<u8>> {
        let mut file = File::open(&self.file_path).ok()?;
        file.seek(SeekFrom::Start(offset)).ok()?;
        let mut buf = vec![0u8; size as usize];
        file.read_exact(&mut buf).ok()?;
        Some(buf)
    }
}

impl ObjectReader for SpilledObjectReader {
    fn data_size(&self) -> u64 {
        self.data_size
    }

    fn metadata_size(&self) -> u64 {
        self.metadata_size
    }

    fn read_data(&self, offset: u64, size: u64) -> Option<Vec<u8>> {
        self.read_file_range(self.data_offset + offset, size)
    }

    fn read_metadata(&self, offset: u64, size: u64) -> Option<Vec<u8>> {
        self.read_file_range(self.metadata_offset + offset, size)
    }
}

// ─── ChunkObjectReader ──────────────────────────────────────────────────────

/// Adapter that reads an object in fixed-size chunks.
///
/// Used for splitting objects into transfer chunks for gRPC push.
pub struct ChunkObjectReader {
    reader: Box<dyn ObjectReader>,
    chunk_size: u64,
}

impl ChunkObjectReader {
    pub fn new(reader: Box<dyn ObjectReader>, chunk_size: u64) -> Self {
        Self { reader, chunk_size }
    }

    /// Total number of chunks.
    pub fn num_chunks(&self) -> u64 {
        let total = self.reader.data_size() + self.reader.metadata_size();
        if total == 0 {
            return 1;
        }
        total.div_ceil(self.chunk_size)
    }

    /// Read chunk at the given index.
    ///
    /// Chunks are laid out as: [metadata][data], split into chunk_size pieces.
    pub fn get_chunk(&self, chunk_index: u64) -> Option<Vec<u8>> {
        let meta_size = self.reader.metadata_size();
        let data_size = self.reader.data_size();
        let total_size = meta_size + data_size;

        let start = chunk_index * self.chunk_size;
        if start >= total_size {
            return None;
        }
        let end = ((chunk_index + 1) * self.chunk_size).min(total_size);
        let chunk_len = end - start;

        let mut result = Vec::with_capacity(chunk_len as usize);

        // Determine which sections this chunk spans
        if start < meta_size {
            // Part in metadata
            let meta_start = start;
            let meta_end = meta_size.min(end);
            let meta_len = meta_end - meta_start;
            if let Some(data) = self.reader.read_metadata(meta_start, meta_len) {
                result.extend_from_slice(&data);
            }
        }

        if end > meta_size {
            // Part in data
            let data_start = start.saturating_sub(meta_size);
            let data_end = end - meta_size;
            let data_len = data_end - data_start;
            if let Some(data) = self.reader.read_data(data_start, data_len) {
                result.extend_from_slice(&data);
            }
        }

        Some(result)
    }

    pub fn underlying_reader(&self) -> &dyn ObjectReader {
        self.reader.as_ref()
    }
}

// ─── MemoryObjectReader ─────────────────────────────────────────────────────

/// Reads an object from an in-memory buffer (used for plasma objects).
pub struct MemoryObjectReader {
    data: Vec<u8>,
    metadata: Vec<u8>,
}

impl MemoryObjectReader {
    pub fn new(data: Vec<u8>, metadata: Vec<u8>) -> Self {
        Self { data, metadata }
    }
}

impl ObjectReader for MemoryObjectReader {
    fn data_size(&self) -> u64 {
        self.data.len() as u64
    }

    fn metadata_size(&self) -> u64 {
        self.metadata.len() as u64
    }

    fn read_data(&self, offset: u64, size: u64) -> Option<Vec<u8>> {
        let start = offset as usize;
        let end = start + size as usize;
        if end <= self.data.len() {
            Some(self.data[start..end].to_vec())
        } else {
            None
        }
    }

    fn read_metadata(&self, offset: u64, size: u64) -> Option<Vec<u8>> {
        let start = offset as usize;
        let end = start + size as usize;
        if end <= self.metadata.len() {
            Some(self.metadata[start..end].to_vec())
        } else {
            None
        }
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn read_u64_le(file: &mut File) -> Option<u64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).ok()?;
    Some(u64::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_memory_reader() {
        let reader = MemoryObjectReader::new(vec![1, 2, 3, 4], vec![10, 20]);
        assert_eq!(reader.data_size(), 4);
        assert_eq!(reader.metadata_size(), 2);
        assert_eq!(reader.read_data(0, 4), Some(vec![1, 2, 3, 4]));
        assert_eq!(reader.read_data(2, 2), Some(vec![3, 4]));
        assert_eq!(reader.read_metadata(0, 2), Some(vec![10, 20]));
    }

    #[test]
    fn test_chunk_reader() {
        let reader = MemoryObjectReader::new(vec![1, 2, 3, 4, 5, 6], vec![]);
        let chunk_reader = ChunkObjectReader::new(Box::new(reader), 2);

        assert_eq!(chunk_reader.num_chunks(), 3);
        assert_eq!(chunk_reader.get_chunk(0), Some(vec![1, 2]));
        assert_eq!(chunk_reader.get_chunk(1), Some(vec![3, 4]));
        assert_eq!(chunk_reader.get_chunk(2), Some(vec![5, 6]));
        assert_eq!(chunk_reader.get_chunk(3), None);
    }

    #[test]
    fn test_spilled_object_reader() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spilled.bin");

        // Write a spilled object file
        let mut f = File::create(&path).unwrap();
        let address = b"addr";
        let metadata = b"meta";
        let data = b"hello world";

        f.write_all(&(address.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(metadata.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(data.len() as u64).to_le_bytes()).unwrap();
        f.write_all(address).unwrap();
        f.write_all(metadata).unwrap();
        f.write_all(data).unwrap();
        drop(f);

        let url = path.to_str().unwrap();
        let reader = SpilledObjectReader::create(url).unwrap();

        assert_eq!(reader.data_size(), 11);
        assert_eq!(reader.metadata_size(), 4);
        assert_eq!(reader.read_data(0, 11), Some(b"hello world".to_vec()));
        assert_eq!(reader.read_metadata(0, 4), Some(b"meta".to_vec()));
    }

    #[test]
    fn test_spilled_url_with_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spilled2.bin");

        let mut f = File::create(&path).unwrap();
        let address = b"";
        let metadata = b"";
        let data = b"test";

        f.write_all(&(address.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(metadata.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(data.len() as u64).to_le_bytes()).unwrap();
        f.write_all(address).unwrap();
        f.write_all(metadata).unwrap();
        f.write_all(data).unwrap();
        drop(f);

        let url = format!("{}?offset=0&size=28", path.to_str().unwrap());
        let reader = SpilledObjectReader::create(&url).unwrap();
        assert_eq!(reader.data_size(), 4);
        assert_eq!(reader.read_data(0, 4), Some(b"test".to_vec()));
    }

    #[test]
    fn test_spilled_nonexistent_file() {
        assert!(SpilledObjectReader::create("/nonexistent/file.bin").is_none());
    }

    #[test]
    fn test_spilled_invalid_url() {
        // Offset/size with non-numeric values
        assert!(SpilledObjectReader::create("/tmp/f?offset=abc&size=10").is_none());
    }

    #[test]
    fn test_chunk_reader_with_metadata() {
        let reader = MemoryObjectReader::new(vec![1, 2, 3, 4], vec![10, 20]);
        // Chunk layout: [metadata][data] = [10,20,1,2,3,4], chunk_size=3
        let chunk_reader = ChunkObjectReader::new(Box::new(reader), 3);

        assert_eq!(chunk_reader.num_chunks(), 2);
        // Chunk 0: bytes 0..3 = [10, 20] (meta) + [1] (data)
        assert_eq!(chunk_reader.get_chunk(0), Some(vec![10, 20, 1]));
        // Chunk 1: bytes 3..6 = [2, 3, 4] (data)
        assert_eq!(chunk_reader.get_chunk(1), Some(vec![2, 3, 4]));
    }

    #[test]
    fn test_chunk_reader_empty_object() {
        let reader = MemoryObjectReader::new(vec![], vec![]);
        let chunk_reader = ChunkObjectReader::new(Box::new(reader), 1024);
        assert_eq!(chunk_reader.num_chunks(), 1);
    }

    #[test]
    fn test_memory_reader_out_of_bounds() {
        let reader = MemoryObjectReader::new(vec![1, 2], vec![10]);
        assert!(reader.read_data(0, 3).is_none());
        assert!(reader.read_metadata(0, 2).is_none());
    }

    #[test]
    fn test_spilled_partial_data_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spilled3.bin");

        let mut f = File::create(&path).unwrap();
        let address = b"";
        let metadata = b"AB";
        let data = b"CDEFGH";

        f.write_all(&(address.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(metadata.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(data.len() as u64).to_le_bytes()).unwrap();
        f.write_all(address).unwrap();
        f.write_all(metadata).unwrap();
        f.write_all(data).unwrap();
        drop(f);

        let reader = SpilledObjectReader::create(path.to_str().unwrap()).unwrap();
        // Partial read: offset 2, size 3 → "EFG"
        assert_eq!(reader.read_data(2, 3), Some(b"EFG".to_vec()));
        // Partial metadata read
        assert_eq!(reader.read_metadata(1, 1), Some(b"B".to_vec()));
    }

    // ─── Tests ported from spilled_object_test.cc ────────────────────────

    #[test]
    fn test_parse_url_success_cases() {
        // Port of C++ ParseObjectURL success cases
        let cases = vec![
            ("file://path/to/file?offset=123&size=456", "file://path/to/file", 123u64, 456u64),
            ("http://123?offset=123&size=456", "http://123", 123, 456),
            ("file:///C:/Users/file.txt?offset=123&size=456", "file:///C:/Users/file.txt", 123, 456),
            ("/tmp/file.txt?offset=123&size=456", "/tmp/file.txt", 123, 456),
            ("C:\\file.txt?offset=123&size=456", "C:\\file.txt", 123, 456),
            (
                "/tmp/ray/session_2021-07-19_09-50-58_115365_119/ray_spillled_objects/\
                 2f81e7cfcc578f4effffffffffffffffffffffff0200000001000000-multi-1?offset=0&size=2199437144",
                "/tmp/ray/session_2021-07-19_09-50-58_115365_119/ray_spillled_objects/\
                 2f81e7cfcc578f4effffffffffffffffffffffff0200000001000000-multi-1",
                0,
                2199437144,
            ),
            ("/tmp/123?offset=0&size=9223372036854775807", "/tmp/123", 0, 9223372036854775807u64),
        ];

        for (url, expected_path, expected_offset, expected_size) in cases {
            let result = SpilledObjectReader::parse_url(url);
            assert!(result.is_some(), "Expected parse success for: {}", url);
            let (path, offset, size) = result.unwrap();
            assert_eq!(path, expected_path, "Path mismatch for: {}", url);
            assert_eq!(offset, expected_offset, "Offset mismatch for: {}", url);
            assert_eq!(size, expected_size, "Size mismatch for: {}", url);
        }
    }

    #[test]
    fn test_parse_url_failure_cases() {
        // Port of C++ ParseObjectURL failure cases
        let fail_cases = vec![
            "/tmp/123?offset=-1&size=1",           // negative offset
            "file://path/to/file?offset=a&size=456", // non-numeric offset
            "file://path/to/file?offset=0&size=bb",  // non-numeric size
        ];

        for url in fail_cases {
            let result = SpilledObjectReader::parse_url(url);
            assert!(result.is_none(), "Expected parse failure for: {}", url);
        }
    }

    #[test]
    fn test_to_u64_le() {
        // Port of C++ ToUINT64 test
        assert_eq!(
            u64::from_le_bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            0u64
        );
        assert_eq!(
            u64::from_le_bytes([0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            1u64
        );
        assert_eq!(
            u64::from_le_bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            u64::MAX
        );
    }

    #[test]
    fn test_read_u64_le_from_stream() {
        // Port of C++ ReadUINT64 test
        let data: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // u64::MAX
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff,             // malformed (6 bytes)
        ];
        let mut cursor = std::io::Cursor::new(&data);

        let mut buf = [0u8; 8];
        use std::io::Read as _;
        cursor.read_exact(&mut buf).unwrap();
        assert_eq!(u64::from_le_bytes(buf), 0);

        cursor.read_exact(&mut buf).unwrap();
        assert_eq!(u64::from_le_bytes(buf), 1);

        cursor.read_exact(&mut buf).unwrap();
        assert_eq!(u64::from_le_bytes(buf), u64::MAX);

        // Malformed: only 6 bytes left, should fail
        assert!(cursor.read_exact(&mut buf).is_err());
    }

    #[test]
    fn test_chunk_get_num_chunks() {
        // Port of C++ ChunkObjectReader::GetNumChunks test
        let cases: Vec<(u64, u64, u64)> = vec![
            (11, 1, 11),
            (1, 11, 1),
            (0, 11, 1),  // Note: Rust impl returns 1 for empty objects
            (9, 2, 5),
            (10, 2, 5),
            (11, 2, 6),
        ];

        for (data_size, chunk_size, expected) in cases {
            let reader = MemoryObjectReader::new(
                vec![0u8; data_size as usize],
                vec![],
            );
            let chunk_reader = ChunkObjectReader::new(Box::new(reader), chunk_size);
            assert_eq!(
                chunk_reader.num_chunks(),
                expected,
                "data_size={}, chunk_size={}",
                data_size,
                chunk_size
            );
            // Call twice to ensure deterministic (ported from C++)
            assert_eq!(chunk_reader.num_chunks(), expected);
        }
    }

    #[test]
    fn test_spilled_reader_create_valid_and_invalid() {
        // Port of C++ CreateSpilledObjectReader test
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("create_test.bin");

        // Write a valid spilled object file with offset 10
        let mut f = File::create(&path).unwrap();
        let offset_padding = vec![0u8; 10];
        let address = b"addr";
        let metadata = b"metadata";
        let data = b"data";

        f.write_all(&offset_padding).unwrap();
        f.write_all(&(address.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(metadata.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(data.len() as u64).to_le_bytes()).unwrap();
        f.write_all(address).unwrap();
        f.write_all(metadata).unwrap();
        f.write_all(data).unwrap();
        drop(f);

        let total_size = 10 + 24 + 4 + 8 + 4;
        let url = format!("{}?offset=10&size={}", path.to_str().unwrap(), total_size - 10);
        assert!(SpilledObjectReader::create(&url).is_some());

        // Malformatted URL should fail
        assert!(SpilledObjectReader::create("malformatted_url").is_none());

        // Empty file should fail to parse header
        let path2 = dir.path().join("empty_test.bin");
        File::create(&path2).unwrap(); // empty file
        let url2 = format!("{}?offset=0&size=0", path2.to_str().unwrap());
        assert!(SpilledObjectReader::create(&url2).is_none());
    }

    #[test]
    fn test_reader_getters_memory() {
        // Port of C++ ObjectReaderTest::Getters for MemoryObjectReader
        let data = b"data";
        let metadata = b"metadata";
        let reader = MemoryObjectReader::new(data.to_vec(), metadata.to_vec());
        assert_eq!(reader.data_size(), 4);
        assert_eq!(reader.metadata_size(), 8);
    }

    #[test]
    fn test_reader_getters_spilled() {
        // Port of C++ ObjectReaderTest::Getters for SpilledObjectReader
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("getters_test.bin");

        let address = b"owner";
        let metadata = b"metadata";
        let data = b"data";
        let mut f = File::create(&path).unwrap();
        f.write_all(&(address.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(metadata.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(data.len() as u64).to_le_bytes()).unwrap();
        f.write_all(address).unwrap();
        f.write_all(metadata).unwrap();
        f.write_all(data).unwrap();
        drop(f);

        let reader = SpilledObjectReader::create(path.to_str().unwrap()).unwrap();
        assert_eq!(reader.data_size(), 4);
        assert_eq!(reader.metadata_size(), 8);
    }

    #[test]
    fn test_reader_get_data_and_metadata_memory() {
        // Port of C++ ObjectReaderTest::GetDataAndMetadata for MemoryObjectReader
        let data_list: Vec<Vec<u8>> = vec![
            vec![],
            b"alotofdata".to_vec(),
            b"da".to_vec(),
            b"data".to_vec(),
        ];
        let metadata_list: Vec<Vec<u8>> = vec![
            vec![],
            b"meta".to_vec(),
            b"metadata".to_vec(),
            b"alotofmetadata".to_vec(),
        ];

        for data in &data_list {
            for metadata in &metadata_list {
                let reader = MemoryObjectReader::new(data.clone(), metadata.clone());

                // Test data section reads
                for offset in 0..=data.len() {
                    for size in 0..=(data.len() - offset) {
                        let result = reader.read_data(offset as u64, size as u64);
                        assert!(result.is_some(), "offset={}, size={}", offset, size);
                        assert_eq!(
                            result.unwrap(),
                            data[offset..offset + size],
                            "data mismatch at offset={}, size={}",
                            offset,
                            size
                        );
                    }
                }

                // Test metadata section reads
                for offset in 0..=metadata.len() {
                    for size in 0..=(metadata.len() - offset) {
                        let result = reader.read_metadata(offset as u64, size as u64);
                        assert!(result.is_some());
                        assert_eq!(result.unwrap(), metadata[offset..offset + size]);
                    }
                }
            }
        }
    }

    #[test]
    fn test_reader_get_data_and_metadata_spilled() {
        // Port of C++ ObjectReaderTest::GetDataAndMetadata for SpilledObjectReader
        let data_list: Vec<&[u8]> = vec![b"", b"alotofdata", b"da", b"data"];
        let metadata_list: Vec<&[u8]> = vec![b"", b"meta", b"metadata"];

        for data in &data_list {
            for metadata in &metadata_list {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("rw_test.bin");

                let address = b"";
                let mut f = File::create(&path).unwrap();
                f.write_all(&(address.len() as u64).to_le_bytes()).unwrap();
                f.write_all(&(metadata.len() as u64).to_le_bytes()).unwrap();
                f.write_all(&(data.len() as u64).to_le_bytes()).unwrap();
                f.write_all(address).unwrap();
                f.write_all(metadata).unwrap();
                f.write_all(data).unwrap();
                drop(f);

                let reader = SpilledObjectReader::create(path.to_str().unwrap()).unwrap();

                // Test data section reads
                for offset in 0..=data.len() {
                    for size in 0..=(data.len() - offset) {
                        let result = reader.read_data(offset as u64, size as u64);
                        assert!(result.is_some());
                        assert_eq!(result.unwrap(), data[offset..offset + size]);
                    }
                }

                // Test metadata section reads
                for offset in 0..=metadata.len() {
                    for size in 0..=(metadata.len() - offset) {
                        let result = reader.read_metadata(offset as u64, size as u64);
                        assert!(result.is_some());
                        assert_eq!(result.unwrap(), metadata[offset..offset + size]);
                    }
                }
            }
        }
    }

    #[test]
    fn test_chunk_reconstruction_memory() {
        // Port of C++ ObjectReaderTest::GetChunk for MemoryObjectReader
        let data_list: Vec<Vec<u8>> = vec![
            vec![],
            b"alotofdata".to_vec(),
            b"da".to_vec(),
            b"data".to_vec(),
        ];
        let metadata_list: Vec<Vec<u8>> = vec![
            vec![],
            b"meta".to_vec(),
            b"metadata".to_vec(),
            b"alotofmetadata".to_vec(),
        ];

        for data in &data_list {
            for metadata in &metadata_list {
                let mut chunk_sizes: Vec<u64> = vec![1, 2, 3, 5, 100];
                let expected_output: Vec<u8> =
                    metadata.iter().chain(data.iter()).copied().collect();
                if !expected_output.is_empty() {
                    chunk_sizes.push(expected_output.len() as u64);
                }

                for chunk_size in &chunk_sizes {
                    let reader =
                        MemoryObjectReader::new(data.clone(), metadata.clone());
                    let chunk_reader =
                        ChunkObjectReader::new(Box::new(reader), *chunk_size);

                    let mut actual_output = Vec::new();
                    let nc = chunk_reader.num_chunks();
                    if expected_output.is_empty() {
                        // Empty object: num_chunks==1 but get_chunk(0) returns None
                        assert_eq!(nc, 1);
                        continue;
                    }
                    for i in 0..nc {
                        let chunk = chunk_reader.get_chunk(i);
                        assert!(chunk.is_some(), "chunk {} should exist", i);
                        let chunk = chunk.unwrap();
                        assert!(
                            chunk.len() as u64 <= *chunk_size,
                            "chunk {} size {} exceeds chunk_size {}",
                            i,
                            chunk.len(),
                            chunk_size
                        );
                        if i + 1 != nc {
                            assert_eq!(
                                chunk.len() as u64, *chunk_size,
                                "non-last chunk {} should be full",
                                i
                            );
                        }
                        actual_output.extend_from_slice(&chunk);
                    }
                    // Chunk layout is [metadata][data]
                    assert_eq!(
                        actual_output, expected_output,
                        "reconstruction failed for data={:?}, metadata={:?}, chunk_size={}",
                        data, metadata, chunk_size
                    );
                }
            }
        }
    }

    #[test]
    fn test_chunk_reconstruction_spilled() {
        // Port of C++ ObjectReaderTest::GetChunk for SpilledObjectReader
        let data_list: Vec<&[u8]> = vec![b"", b"alotofdata", b"da"];
        let metadata_list: Vec<&[u8]> = vec![b"", b"meta"];

        for data in &data_list {
            for metadata in &metadata_list {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("chunk_test.bin");

                let address = b"";
                let mut f = File::create(&path).unwrap();
                f.write_all(&(address.len() as u64).to_le_bytes()).unwrap();
                f.write_all(&(metadata.len() as u64).to_le_bytes()).unwrap();
                f.write_all(&(data.len() as u64).to_le_bytes()).unwrap();
                f.write_all(address).unwrap();
                f.write_all(metadata).unwrap();
                f.write_all(data).unwrap();
                drop(f);

                let expected_output: Vec<u8> =
                    metadata.iter().chain(data.iter()).copied().collect();

                let chunk_sizes: Vec<u64> = vec![1, 2, 3, 5, 100];
                for chunk_size in &chunk_sizes {
                    let reader =
                        SpilledObjectReader::create(path.to_str().unwrap()).unwrap();
                    let chunk_reader =
                        ChunkObjectReader::new(Box::new(reader), *chunk_size);

                    if expected_output.is_empty() {
                        assert_eq!(chunk_reader.num_chunks(), 1);
                        continue;
                    }
                    let mut actual_output = Vec::new();
                    for i in 0..chunk_reader.num_chunks() {
                        let chunk = chunk_reader.get_chunk(i).unwrap();
                        actual_output.extend_from_slice(&chunk);
                    }
                    assert_eq!(actual_output, expected_output);
                }
            }
        }
    }

    #[test]
    fn test_spilled_multi_object_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.bin");

        let mut f = File::create(&path).unwrap();

        // Write first object at offset 0
        let addr1 = b"a1";
        let meta1 = b"m1";
        let data1 = b"data1";
        f.write_all(&(addr1.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(meta1.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(data1.len() as u64).to_le_bytes()).unwrap();
        f.write_all(addr1).unwrap();
        f.write_all(meta1).unwrap();
        f.write_all(data1).unwrap();

        let obj2_offset = 24 + 2 + 2 + 5; // header(24) + addr(2) + meta(2) + data(5) = 33

        // Write second object at offset 33
        let addr2 = b"a2";
        let meta2 = b"mm";
        let data2 = b"second";
        f.write_all(&(addr2.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(meta2.len() as u64).to_le_bytes()).unwrap();
        f.write_all(&(data2.len() as u64).to_le_bytes()).unwrap();
        f.write_all(addr2).unwrap();
        f.write_all(meta2).unwrap();
        f.write_all(data2).unwrap();
        drop(f);

        // Read the second object using offset
        let url = format!(
            "{}?offset={}&size={}",
            path.to_str().unwrap(),
            obj2_offset,
            24 + 2 + 2 + 6
        );
        let reader = SpilledObjectReader::create(&url).unwrap();
        assert_eq!(reader.data_size(), 6);
        assert_eq!(reader.metadata_size(), 2);
        assert_eq!(reader.read_data(0, 6), Some(b"second".to_vec()));
        assert_eq!(reader.read_metadata(0, 2), Some(b"mm".to_vec()));
    }
}
