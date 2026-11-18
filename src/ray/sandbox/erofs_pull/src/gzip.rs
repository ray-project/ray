//! Parallel gzip decoding with rapidgzip, through our own small C API
//! (gzip.cpp), of a layer that may still be downloading.

use std::ffi::{c_char, c_int, c_void, CStr};
use std::io::{self, Read};
use std::sync::Arc;

use crate::fetch::Blob;

extern "C" {
    fn erofs_pull_gzip_open(
        fd: c_int,
        size: u64,
        wait: extern "C" fn(*mut c_void, u64) -> c_int,
        context: *mut c_void,
        parallelism: u32,
        chunk_size: u64,
        error: *mut c_char,
        error_len: usize,
    ) -> *mut c_void;
    fn erofs_pull_gzip_read(
        reader: *mut c_void,
        buffer: *mut u8,
        len: usize,
        error: *mut c_char,
        error_len: usize,
    ) -> i64;
    fn erofs_pull_gzip_close(reader: *mut c_void);
}

/// rapidgzip's chunk size: 4 MiB of compressed input, its default.
const CHUNK_SIZE: u64 = 4 << 20;

/// Called from rapidgzip's threads before they read: waits for the bytes to
/// arrive. `context` is the blob a GzipReader keeps alive.
extern "C" fn wait(context: *mut c_void, end: u64) -> c_int {
    let blob = unsafe { &*(context as *const Blob) };
    match blob.wait_until(end) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

fn error_message(buffer: &[c_char]) -> String {
    unsafe { CStr::from_ptr(buffer.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

/// A gzip-compressed blob, decoded on `threads` threads (0: one per core).
pub struct GzipReader {
    handle: *mut c_void,
    // Keeps the blob, which rapidgzip's threads call back into, alive until
    // erofs_pull_gzip_close has stopped them.
    _blob: Arc<Blob>,
}

// The handle is used by one thread at a time; rapidgzip's own threads only
// call `wait`, and Blob is Sync.
unsafe impl Send for GzipReader {}

impl GzipReader {
    pub fn open(blob: Arc<Blob>, threads: u32) -> io::Result<Self> {
        let mut error = [0 as c_char; 512];
        let handle = unsafe {
            erofs_pull_gzip_open(
                blob.raw_fd(),
                blob.size,
                wait,
                Arc::as_ptr(&blob) as *mut c_void,
                threads,
                CHUNK_SIZE,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        if handle.is_null() {
            return Err(io::Error::other(format!("rapidgzip: {}", error_message(&error))));
        }
        Ok(GzipReader { handle, _blob: blob })
    }
}

impl Read for GzipReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let mut error = [0 as c_char; 512];
        let n = unsafe {
            erofs_pull_gzip_read(
                self.handle,
                buffer.as_mut_ptr(),
                buffer.len(),
                error.as_mut_ptr(),
                error.len(),
            )
        };
        if n < 0 {
            return Err(io::Error::other(format!("rapidgzip: {}", error_message(&error))));
        }
        Ok(n as usize)
    }
}

impl Drop for GzipReader {
    fn drop(&mut self) {
        unsafe { erofs_pull_gzip_close(self.handle) };
    }
}
