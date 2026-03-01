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
