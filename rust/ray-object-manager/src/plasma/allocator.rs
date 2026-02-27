// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Plasma memory allocator using mmap/shm.
//!
//! Replaces `src/ray/object_manager/plasma/plasma_allocator.h/cc`.
//!
//! # Safety
//!
//! This module contains `unsafe` code for mmap/munmap/shm_open operations.
//! All unsafe operations are isolated in small, audited functions.

use std::os::fd::RawFd;
use std::ptr;
use std::sync::atomic::{AtomicI64, Ordering};

/// A memory allocation from the plasma allocator.
///
/// Represents a region of memory-mapped shared memory.
/// Move-only — dropping an Allocation does NOT free the memory;
/// it must be explicitly returned to the allocator via `free()`.
pub struct Allocation {
    /// Pointer to the allocated memory region.
    pub address: *mut u8,
    /// Number of bytes allocated.
    pub size: i64,
    /// File descriptor of the backing mmap'd file.
    pub fd: RawFd,
    /// Byte offset within the mmap'd region.
    pub offset: isize,
    /// Device number (0 = host, >0 = GPU).
    pub device_num: i32,
    /// Total size of the mmap'd region.
    pub mmap_size: i64,
    /// Whether this was allocated from the fallback (disk) allocator.
    pub fallback_allocated: bool,
}

// Allocation is Send because the raw pointer points to shared memory
// that is accessed by multiple processes (by design).
// Safety: Access is coordinated by the plasma protocol.
unsafe impl Send for Allocation {}

/// Trait for plasma memory allocators.
pub trait IAllocator: Send + Sync {
    /// Allocate `bytes` of shared memory (primary allocator).
    fn allocate(&self, bytes: usize) -> Option<Allocation>;

    /// Allocate from the fallback (disk-backed) allocator.
    fn fallback_allocate(&self, bytes: usize) -> Option<Allocation>;

    /// Free a previously-allocated region.
    fn free(&self, allocation: Allocation);

    /// Maximum memory footprint in bytes.
    fn footprint_limit(&self) -> i64;

    /// Currently allocated bytes (primary).
    fn allocated(&self) -> i64;

    /// Currently allocated bytes (fallback/disk).
    fn fallback_allocated(&self) -> i64;
}

/// The default plasma allocator using mmap for shared memory.
///
/// - Primary: `/dev/shm` (Linux) or `mmap(MAP_SHARED)` — RAM-backed
/// - Fallback: disk-backed mmap files in `fallback_directory`
pub struct PlasmaAllocator {
    footprint_limit: i64,
    alignment: usize,
    allocated: AtomicI64,
    fallback_allocated_bytes: AtomicI64,
    plasma_directory: String,
    fallback_directory: String,
    huge_pages: bool,
}

impl PlasmaAllocator {
    /// Create a new allocator with the given memory limit.
    pub fn new(
        footprint_limit: i64,
        plasma_directory: &str,
        fallback_directory: &str,
        huge_pages: bool,
    ) -> Self {
        Self {
            footprint_limit,
            alignment: 64, // Cache-line aligned
            allocated: AtomicI64::new(0),
            fallback_allocated_bytes: AtomicI64::new(0),
            plasma_directory: plasma_directory.to_string(),
            fallback_directory: fallback_directory.to_string(),
            huge_pages,
        }
    }

    /// Create a memory-mapped allocation backed by a file in the given directory.
    ///
    /// # Safety
    /// Uses mmap which produces raw pointers to shared memory.
    fn mmap_allocate(&self, bytes: usize, directory: &str) -> Option<Allocation> {
        let aligned_size = self.align_up(bytes);

        // Create a temporary file for the mmap backing
        let template = format!("{}/ray-plasma-XXXXXX", directory);
        let fd = create_and_mmap_fd(&template, aligned_size)?;

        // mmap the file
        let address = unsafe {
            #[allow(unused_mut)]
            let mut flags = libc::MAP_SHARED;
            if self.huge_pages {
                #[cfg(target_os = "linux")]
                {
                    flags |= libc::MAP_HUGETLB;
                }
            }

            let ptr = libc::mmap(
                ptr::null_mut(),
                aligned_size,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                fd,
                0,
            );

            if ptr == libc::MAP_FAILED {
                libc::close(fd);
                return None;
            }
            ptr as *mut u8
        };

        Some(Allocation {
            address,
            size: aligned_size as i64,
            fd,
            offset: 0,
            device_num: 0,
            mmap_size: aligned_size as i64,
            fallback_allocated: false,
        })
    }

    fn align_up(&self, size: usize) -> usize {
        (size + self.alignment - 1) & !(self.alignment - 1)
    }
}

impl IAllocator for PlasmaAllocator {
    fn allocate(&self, bytes: usize) -> Option<Allocation> {
        let aligned = self.align_up(bytes) as i64;
        let current = self.allocated.load(Ordering::Relaxed);
        if current + aligned > self.footprint_limit {
            return None;
        }

        let alloc = self.mmap_allocate(bytes, &self.plasma_directory)?;
        self.allocated.fetch_add(alloc.size, Ordering::Relaxed);
        Some(alloc)
    }

    fn fallback_allocate(&self, bytes: usize) -> Option<Allocation> {
        if self.fallback_directory.is_empty() {
            return None;
        }
        let mut alloc = self.mmap_allocate(bytes, &self.fallback_directory)?;
        alloc.fallback_allocated = true;
        self.fallback_allocated_bytes
            .fetch_add(alloc.size, Ordering::Relaxed);
        Some(alloc)
    }

    fn free(&self, allocation: Allocation) {
        let size = allocation.size;
        let is_fallback = allocation.fallback_allocated;

        // Safety: munmap + close the file descriptor.
        unsafe {
            if !allocation.address.is_null() {
                libc::munmap(allocation.address as *mut libc::c_void, size as usize);
            }
            if allocation.fd >= 0 {
                libc::close(allocation.fd);
            }
        }

        if is_fallback {
            self.fallback_allocated_bytes
                .fetch_sub(size, Ordering::Relaxed);
        } else {
            self.allocated.fetch_sub(size, Ordering::Relaxed);
        }
    }

    fn footprint_limit(&self) -> i64 {
        self.footprint_limit
    }

    fn allocated(&self) -> i64 {
        self.allocated.load(Ordering::Relaxed)
    }

    fn fallback_allocated(&self) -> i64 {
        self.fallback_allocated_bytes.load(Ordering::Relaxed)
    }
}

/// Create a temporary file, resize it, and return its fd.
fn create_and_mmap_fd(template: &str, size: usize) -> Option<RawFd> {
    use std::ffi::CString;

    // Use a simpler approach: create a file via shm_open-style path
    let path = CString::new(template.replace("XXXXXX", &format!("{}", std::process::id()))).ok()?;

    unsafe {
        let fd = libc::open(
            path.as_ptr(),
            libc::O_CREAT | libc::O_RDWR | libc::O_EXCL,
            0o600,
        );
        if fd < 0 {
            // Fallback: try without O_EXCL in case file exists
            let fd = libc::open(path.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600);
            if fd < 0 {
                return None;
            }
            libc::unlink(path.as_ptr()); // Delete file on close
            if libc::ftruncate(fd, size as libc::off_t) != 0 {
                libc::close(fd);
                return None;
            }
            return Some(fd);
        }

        libc::unlink(path.as_ptr()); // Delete name so file disappears when fd closes
        if libc::ftruncate(fd, size as libc::off_t) != 0 {
            libc::close(fd);
            return None;
        }
        Some(fd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocator_basic() {
        let dir = tempfile::tempdir().unwrap();
        let allocator = PlasmaAllocator::new(
            1024 * 1024, // 1MB limit
            dir.path().to_str().unwrap(),
            "",
            false,
        );

        assert_eq!(allocator.allocated(), 0);
        assert_eq!(allocator.footprint_limit(), 1024 * 1024);

        // Allocate some memory
        let alloc = allocator.allocate(4096);
        assert!(alloc.is_some());

        let alloc = alloc.unwrap();
        assert!(!alloc.address.is_null());
        assert!(alloc.size >= 4096);
        assert!(allocator.allocated() > 0);

        // Free it
        allocator.free(alloc);
        assert_eq!(allocator.allocated(), 0);
    }

    #[test]
    fn test_allocator_oom() {
        let dir = tempfile::tempdir().unwrap();
        let allocator = PlasmaAllocator::new(
            100, // Very small limit
            dir.path().to_str().unwrap(),
            "",
            false,
        );

        // Should fail: requesting more than limit
        let alloc = allocator.allocate(1024);
        assert!(alloc.is_none());
    }
}
