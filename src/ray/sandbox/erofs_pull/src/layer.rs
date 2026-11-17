//! Streams one layer's tar into the shared output image.
//!
//! Regular file data goes straight to its final place in the image (each file
//! starts on a block boundary); everything else is recorded as an `Entry` to
//! be applied to the tree once all layers are read.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, Ordering};

use tar::EntryType;

/// The output image: data blocks are handed out from block 1 on (block 0
/// holds the superblock). The block size is the host's page size: gVisor
/// only mounts an image whose blocks are a multiple of it.
pub struct Out {
    pub file: File,
    pub block: u64,
    next_blk: AtomicU64,
}

impl Out {
    pub fn new(file: File, block: u64) -> Self {
        Out {
            file,
            block,
            next_blk: AtomicU64::new(1),
        }
    }

    /// Reserves blocks for `size` bytes and returns the first block.
    pub fn alloc(&self, size: u64) -> u64 {
        self.next_blk.fetch_add(size.div_ceil(self.block), Ordering::Relaxed)
    }

    pub fn blocks(&self) -> u64 {
        self.next_blk.load(Ordering::Relaxed)
    }
}

pub enum Kind {
    Reg {
        blk: u64,
        size: u64,
    },
    Dir,
    Sym(Vec<u8>),
    Hard(Vec<u8>),
    /// Devices and FIFOs: extraction removes what's at the path and creates
    /// nothing, so they replace and vanish.
    Other,
}

pub struct Entry {
    pub path: Vec<u8>,
    pub kind: Kind,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub mtime: u64,
}

/// Reads a stream on a thread of its own, so decompression overlaps the tar
/// parsing and the writes that consume it.
struct Pipe {
    rx: std::sync::mpsc::Receiver<io::Result<Vec<u8>>>,
    buf: Vec<u8>,
    pos: usize,
}

impl Pipe {
    fn spawn(mut src: impl Read + Send + 'static) -> Pipe {
        let (tx, rx) = std::sync::mpsc::sync_channel(16);
        std::thread::spawn(move || loop {
            let mut buf = vec![0u8; 1 << 20];
            let mut filled = 0;
            while filled < buf.len() {
                match src.read(&mut buf[filled..]) {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                }
            }
            buf.truncate(filled);
            if tx.send(Ok(buf)).is_err() || filled == 0 {
                return;
            }
        });
        Pipe {
            rx,
            buf: Vec::new(),
            pos: 0,
        }
    }
}

impl Read for Pipe {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos == self.buf.len() {
            match self.rx.recv() {
                Ok(Ok(buf)) => (self.buf, self.pos) = (buf, 0),
                Ok(Err(e)) => return Err(e),
                Err(_) => return Ok(0),
            }
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Gzip layers decompress with rapidgzip, in parallel across `threads`
/// (0: rapidgzip's default, all cores); its workers read ahead in the blob,
/// waiting where it hasn't arrived yet.
pub fn read_layer(reader: crate::fetch::BlobReader, threads: u32, out: &Out) -> io::Result<Vec<Entry>> {
    let mut buffered = BufReader::with_capacity(1 << 20, reader.clone());
    let head = buffered.fill_buf()?;
    let stream = if head.starts_with(&[0x1f, 0x8b]) {
        let rgz = rapidgzip::ReaderBuilder::new()
            .parallelism(threads)
            .open_cloneable_reader(reader)
            .map_err(|e| io::Error::other(format!("rapidgzip: {e}")))?;
        Pipe::spawn(rgz)
    } else if head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        return Err(io::Error::other("zstd layers are not supported by this prototype"));
    } else {
        Pipe::spawn(buffered)
    };
    let mut archive = tar::Archive::new(stream);
    let mut entries = Vec::new();
    let mut buf = vec![0u8; 1 << 20];
    for entry in archive.entries()? {
        let mut entry = entry?;
        let header = entry.header();
        let mut uid = header.uid().unwrap_or(0) as u32;
        let mut gid = header.gid().unwrap_or(0) as u32;
        let mut mtime = header.mtime().unwrap_or(0);
        let mode = (header.mode().unwrap_or(0) & 0o7777) as u16;
        let etype = header.entry_type();
        if let Ok(Some(pax)) = entry.pax_extensions() {
            for ext in pax.flatten() {
                let v = ext.value().ok().map(|v| v.split('.').next().unwrap_or("").to_string());
                match (ext.key().unwrap_or(""), v.and_then(|v| v.parse::<u64>().ok())) {
                    ("uid", Some(v)) => uid = v as u32,
                    ("gid", Some(v)) => gid = v as u32,
                    ("mtime", Some(v)) => mtime = v,
                    _ => {}
                }
            }
        }
        let path = entry.path_bytes().into_owned();
        let kind = match etype {
            EntryType::Regular | EntryType::Continuous => {
                let size = entry.size();
                let blk = out.alloc(size);
                let mut off = blk * out.block;
                loop {
                    let n = entry.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    out.file.write_all_at(&buf[..n], off)?;
                    off += n as u64;
                }
                if off != blk * out.block + size {
                    return Err(io::Error::other("short file in layer"));
                }
                Kind::Reg { blk, size }
            }
            EntryType::Directory => Kind::Dir,
            EntryType::Symlink => Kind::Sym(entry.link_name_bytes().map(|l| l.into_owned()).unwrap_or_default()),
            EntryType::Link => Kind::Hard(entry.link_name_bytes().map(|l| l.into_owned()).unwrap_or_default()),
            EntryType::Char | EntryType::Block | EntryType::Fifo => Kind::Other,
            _ => continue,
        };
        entries.push(Entry {
            path,
            kind,
            mode,
            uid,
            gid,
            mtime,
        });
    }
    // Drain the rest so a gzip stream's trailer (CRC, size) is checked.
    io::copy(&mut archive.into_inner(), &mut io::sink())?;
    Ok(entries)
}
