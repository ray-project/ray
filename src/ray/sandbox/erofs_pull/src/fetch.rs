//! Parallel ranged downloads of layer blobs into the layer cache.
//!
//! Blobs live in Ray's layer cache (`layer_cache.py`), under its conventions:
//! a blob is looked up or downloaded under an exclusive `flock` on
//! `<hex>.lock`, a download goes to `.partial-<hex>-...` (which the cache
//! removes once abandoned), and a cache hit refreshes the blob's mtime for
//! LRU eviction.
//!
//! Each blob is split into chunks fetched over a shared pool of connections
//! and written in place (`pwrite`) into its partial file. Readers (`BlobReader`)
//! see the blob grow as its contiguous prefix arrives, so decompression and
//! hashing start right away. A blob is renamed to `<hex>` only after its
//! SHA-256 matches. Where the server (or the storage it redirects to)
//! ignores `Range`, the blob downloads in one stream instead.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

pub struct Remote {
    pub agent: ureq::Agent,
    pub headers: Vec<(String, String)>,
    pub auth: Option<String>,
}

pub struct Blob {
    pub digest: String,
    pub size: u64,
    url: String,
    file: File,
    partial: PathBuf,
    final_path: PathBuf,
    cached: bool,
    /// The cache's lock on this blob, held until it is committed or discarded.
    lock: Mutex<Option<File>>,
    chunk: u64,
    resolved: OnceLock<Result<Resolved, String>>,
    state: Mutex<State>,
    cond: Condvar,
}

/// Where a blob's bytes come from, once the registry's redirects are followed.
#[derive(Clone)]
struct Resolved {
    url: String,
    /// Whether to send the registry's headers: not to storage it redirects
    /// to, which (like S3 with a presigned URL) may refuse a second auth.
    auth: bool,
    /// Whether the server answers `Range` requests.
    ranged: bool,
}

struct State {
    written: Vec<u64>, // bytes written per chunk
    first_incomplete: usize,
    prefix: u64, // contiguous bytes available from offset 0
    error: Option<String>,
}

/// The hex part of a `sha256:` digest of exactly 64 lowercase hex digits, as
/// the layer cache accepts (`layer_cache._DIGEST`), or None. The digest
/// comes from the registry's manifest and names files in the cache, so
/// anything else (`sha256:../../x`, say) is refused before it reaches a path.
pub fn digest_hex(digest: &str) -> Option<&str> {
    digest
        .strip_prefix("sha256:")
        .filter(|hex| hex.len() == 64 && hex.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')))
}

impl Blob {
    pub fn open(blob_dir: &str, base_url: &str, digest: &str, size: u64, chunk: u64) -> io::Result<Arc<Blob>> {
        let hex = digest_hex(digest).ok_or_else(|| io::Error::other(format!("unsupported layer digest {digest:?}")))?;
        fs::create_dir_all(blob_dir)?;
        let final_path = PathBuf::from(blob_dir).join(hex);
        let partial = PathBuf::from(blob_dir).join(format!(".partial-{hex}-erofs-pull-{}", std::process::id()));
        let lock = OpenOptions::new()
            .append(true)
            .create(true)
            .open(PathBuf::from(blob_dir).join(format!("{hex}.lock")))?;
        lock.lock()?;
        let (file, cached) = match File::open(&final_path) {
            Ok(f) if f.metadata()?.len() == size => {
                let _ = File::options()
                    .write(true)
                    .open(&final_path)
                    .and_then(|w| w.set_modified(std::time::SystemTime::now()));
                (f, true)
            }
            _ => {
                let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&partial)?;
                f.set_len(size)?;
                (f, false)
            }
        };
        let nchunks = if cached { 0 } else { size.div_ceil(chunk) as usize };
        Ok(Arc::new(Blob {
            digest: digest.to_string(),
            size,
            url: format!("{base_url}{digest}"),
            file,
            partial,
            final_path,
            cached,
            // A cached blob is open; the lock has done its job.
            lock: Mutex::new((!cached).then_some(lock)),
            chunk,
            resolved: OnceLock::new(),
            state: Mutex::new(State {
                written: vec![0; nchunks],
                first_incomplete: 0,
                prefix: if cached { size } else { 0 },
                error: None,
            }),
            cond: Condvar::new(),
        }))
    }

    pub fn cached(&self) -> bool {
        self.cached
    }

    pub fn nchunks(&self) -> usize {
        self.state.lock().unwrap().written.len()
    }

    fn chunk_range(&self, k: usize) -> (u64, u64) {
        let start = k as u64 * self.chunk;
        (start, (start + self.chunk).min(self.size))
    }

    fn fail(&self, msg: String) {
        let mut st = self.state.lock().unwrap();
        st.error.get_or_insert(msg);
        self.cond.notify_all();
    }

    fn wrote(&self, k: usize, n: u64) {
        let mut st = self.state.lock().unwrap();
        st.written[k] += n;
        let mut prefix = st.first_incomplete as u64 * self.chunk;
        while st.first_incomplete < st.written.len() {
            let (s, e) = self.chunk_range(st.first_incomplete);
            let w = st.written[st.first_incomplete];
            prefix = s + w;
            if s + w < e {
                break;
            }
            st.first_incomplete += 1;
        }
        st.prefix = prefix;
        self.cond.notify_all();
    }

    /// Waits until the first `end` bytes are available.
    fn wait_until(&self, end: u64) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        loop {
            if let Some(e) = &st.error {
                return Err(io::Error::other(e.clone()));
            }
            if st.prefix >= end.min(self.size) {
                return Ok(());
            }
            st = self.cond.wait(st).unwrap();
        }
    }

    /// Follows the registry's redirects with a one-byte range request, to find
    /// where the blob's bytes are and whether that server serves ranges.
    fn resolve(&self, remote: &Remote) -> Result<Resolved, String> {
        self.resolved
            .get_or_init(|| {
                let (mut url, mut auth) = (self.url.clone(), true);
                for _hop in 0..5 {
                    let resp = request(remote, &url, auth, Some((0, 0)))?;
                    match resp.status() {
                        301 | 302 | 303 | 307 | 308 => {
                            let loc = resp.header("location").ok_or("redirect without location")?;
                            url = join_url(&url, loc);
                            auth = false;
                        }
                        206 => {
                            return Ok(Resolved {
                                url,
                                auth,
                                ranged: true,
                            })
                        }
                        200 => {
                            return Ok(Resolved {
                                url,
                                auth,
                                ranged: false,
                            })
                        }
                        s => return Err(format!("{}: answered {s}", self.digest)),
                    }
                }
                Err(format!("{}: too many redirects", self.digest))
            })
            .clone()
    }

    /// Downloads chunk `k`: by range, or, where the server ignores ranges,
    /// the whole blob in one stream (chunk 0's task; the others have nothing
    /// to do).
    fn fetch_chunk(&self, remote: &Remote, k: usize) -> Result<(), String> {
        let Resolved { url, auth, ranged } = self.resolve(remote)?;
        if !ranged {
            return if k == 0 {
                self.fetch_whole(remote, &url, auth)
            } else {
                Ok(())
            };
        }
        let (start, end) = self.chunk_range(k);
        let mut last_err = String::new();
        for _attempt in 0..4 {
            let done = self.state.lock().unwrap().written[k];
            let from = start + done;
            if from >= end {
                return Ok(());
            }
            match request(remote, &url, auth, Some((from, end - 1))) {
                Ok(resp) if resp.status() == 206 => {
                    let mut body = resp.into_reader();
                    let mut buf = vec![0u8; 1 << 20];
                    let mut off = from;
                    let res: io::Result<()> = (|| {
                        while off < end {
                            let want = buf.len().min((end - off) as usize);
                            let n = body.read(&mut buf[..want])?;
                            if n == 0 {
                                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short body"));
                            }
                            self.file.write_all_at(&buf[..n], off)?;
                            off += n as u64;
                            self.wrote(k, n as u64);
                        }
                        Ok(())
                    })();
                    match res {
                        Ok(()) => return Ok(()),
                        Err(e) => last_err = e.to_string(),
                    }
                }
                Ok(resp) => last_err = format!("status {}", resp.status()),
                Err(e) => last_err = e,
            }
        }
        Err(format!("{} chunk {k}: {last_err}", self.digest))
    }

    fn fetch_whole(&self, remote: &Remote, url: &str, auth: bool) -> Result<(), String> {
        let fail = |e: String| format!("{}: {e}", self.digest);
        let resp = request(remote, url, auth, None).map_err(fail)?;
        if resp.status() != 200 {
            return Err(fail(format!("status {}", resp.status())));
        }
        let mut body = resp.into_reader();
        let mut buf = vec![0u8; 1 << 20];
        let mut off = 0;
        loop {
            let n = body.read(&mut buf).map_err(|e| fail(e.to_string()))?;
            if n == 0 {
                break;
            }
            if off + n as u64 > self.size {
                return Err(fail("longer than its manifest size".into()));
            }
            self.file
                .write_all_at(&buf[..n], off)
                .map_err(|e| fail(e.to_string()))?;
            // Credit each chunk the bytes that fall in it.
            let (mut pos, end) = (off, off + n as u64);
            while pos < end {
                let k = (pos / self.chunk) as usize;
                let upto = end.min(self.chunk_range(k).1);
                self.wrote(k, upto - pos);
                pos = upto;
            }
            off = end;
        }
        if off != self.size {
            return Err(fail(format!("{off} bytes, not {}", self.size)));
        }
        Ok(())
    }

    /// Once the digest checks out: move the blob to its cache name and
    /// release its lock, right away rather than when the pull ends, so other
    /// pulls never wait on more than this blob's download. Readers keep
    /// reading through the open file. Does nothing once the lock is released.
    pub fn commit(&self) -> io::Result<()> {
        let mut lock = self.lock.lock().unwrap();
        if lock.is_some() {
            use std::os::unix::fs::PermissionsExt;
            self.file.set_permissions(fs::Permissions::from_mode(0o644))?;
            fs::rename(&self.partial, &self.final_path)?;
            lock.take();
        }
        Ok(())
    }

    /// Removes an unverified download and releases its lock; does nothing
    /// to a blob already committed or found in the cache.
    pub fn discard(&self) {
        if self.lock.lock().unwrap().take().is_some() {
            let _ = fs::remove_file(&self.partial);
        }
    }
}

/// Resolves a redirect's `Location` against the URL that answered it.
fn join_url(base: &str, location: &str) -> String {
    if location.contains("://") {
        return location.to_string();
    }
    let scheme_end = base.find("://").map_or(0, |i| i + 3);
    if let Some(rest) = location.strip_prefix("//") {
        return format!("{}{rest}", &base[..scheme_end]);
    }
    let host_end = base[scheme_end..].find('/').map_or(base.len(), |i| scheme_end + i);
    if location.starts_with('/') {
        return format!("{}{location}", &base[..host_end]);
    }
    let path = &base[..host_end.max(base.rfind('/').map_or(0, |i| i + 1))];
    let sep = if path.ends_with('/') { "" } else { "/" };
    format!("{path}{sep}{location}")
}

fn request(remote: &Remote, url: &str, auth: bool, range: Option<(u64, u64)>) -> Result<ureq::Response, String> {
    let mut req = remote.agent.get(url);
    if let Some((from, to)) = range {
        req = req.set("Range", &format!("bytes={from}-{to}"));
    }
    if auth {
        for (k, v) in &remote.headers {
            req = req.set(k, v);
        }
        if let Some(a) = &remote.auth {
            req = req.set("Authorization", a);
        }
    }
    match req.call() {
        Ok(r) => Ok(r),
        Err(ureq::Error::Status(code, r)) => Err(format!("HTTP {code}: {}", r.into_string().unwrap_or_default())),
        Err(e) => Err(e.to_string()),
    }
}

pub fn agent(timeout: Duration) -> ureq::Agent {
    ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(timeout)
        .timeout_read(timeout)
        .max_idle_connections_per_host(64)
        .build()
}

/// Fetch every chunk of `blobs` over `connections` connections.
///
/// Chunks go in the order given: blob by blob, each from its start. The
/// layers are written into the image in that order, so the connections all
/// work on the earliest layer not yet in while the ones before it are
/// written, and later layers queue behind it.
pub fn download_all(remote: Arc<Remote>, blobs: &[Arc<Blob>], connections: usize) -> Vec<std::thread::JoinHandle<()>> {
    let chunks: VecDeque<(usize, usize)> = blobs
        .iter()
        .enumerate()
        .flat_map(|(i, b)| (0..b.nchunks()).map(move |k| (i, k)))
        .collect();
    let threads = connections.min(chunks.len());
    let queue = Arc::new(Mutex::new(chunks));
    (0..threads)
        .map(|_| {
            let queue = queue.clone();
            let blobs = blobs.to_vec();
            let remote = remote.clone();
            std::thread::spawn(move || loop {
                let Some((i, k)) = queue.lock().unwrap().pop_front() else {
                    return;
                };
                if blobs[i].state.lock().unwrap().error.is_some() {
                    continue;
                }
                if let Err(e) = blobs[i].fetch_chunk(&remote, k) {
                    blobs[i].fail(e);
                }
            })
        })
        .collect()
}

/// Reader over a blob that may still be downloading: a read waits until
/// every byte it asks for has arrived. Clones read independently, which is
/// what rapidgzip's parallel workers need.
#[derive(Clone)]
pub struct BlobReader {
    blob: Arc<Blob>,
    pos: u64,
}

impl BlobReader {
    pub fn new(blob: Arc<Blob>) -> Self {
        BlobReader { blob, pos: 0 }
    }
}

impl Read for BlobReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.blob.size {
            return Ok(0);
        }
        let n = (buf.len() as u64).min(self.blob.size - self.pos) as usize;
        self.blob.wait_until(self.pos + n as u64)?;
        let n = self.blob.file.read_at(&mut buf[..n], self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl io::Seek for BlobReader {
    fn seek(&mut self, to: io::SeekFrom) -> io::Result<u64> {
        let pos = match to {
            io::SeekFrom::Start(p) => p as i64,
            io::SeekFrom::Current(d) => self.pos as i64 + d,
            io::SeekFrom::End(d) => self.blob.size as i64 + d,
        };
        if pos < 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "seek before start"));
        }
        self.pos = pos as u64;
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::{digest_hex, join_url};

    #[test]
    fn only_sha256_digests_of_64_hex_digits_name_cache_files() {
        let hex = "0123456789abcdef".repeat(4);
        assert_eq!(digest_hex(&format!("sha256:{hex}")), Some(hex.as_str()));
        for bad in [
            "sha256:../../x".to_string(),
            format!("sha256:{}", &hex[..63]),
            format!("sha256:{hex}0"),
            format!("sha256:{}", hex.to_uppercase()),
            format!("sha256:{}/x", &hex[..62]),
            format!("sha512:{hex}"),
            hex.clone(),
        ] {
            assert_eq!(digest_hex(&bad), None, "{bad}");
        }
    }

    #[test]
    fn redirect_locations_resolve_against_the_answering_url() {
        let base = "https://registry.example/v2/repo/blobs/sha256:abc";
        let cases = [
            ("https://cdn.example/x?sig=1", "https://cdn.example/x?sig=1"),
            ("//cdn.example/x", "https://cdn.example/x"),
            ("/storage/sha256:abc", "https://registry.example/storage/sha256:abc"),
            ("other", "https://registry.example/v2/repo/blobs/other"),
        ];
        for (location, want) in cases {
            assert_eq!(join_url(base, location), want, "{location}");
        }
        assert_eq!(
            join_url("http://127.0.0.1:5000", "/storage/d"),
            "http://127.0.0.1:5000/storage/d"
        );
    }
}
