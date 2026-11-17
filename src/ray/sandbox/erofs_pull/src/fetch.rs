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

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

/// The registry, and how requests reach it and the storage it redirects to.
pub struct Remote {
    direct: ureq::Agent,
    /// Agents that go through a proxy, by URL scheme ("http", "https"), with
    /// the proxy's `Proxy-Authorization` value if it takes credentials.
    proxied: HashMap<String, (ureq::Agent, Option<String>)>,
    /// `no_proxy`: hosts reached directly whatever the proxies.
    no_proxy: Option<String>,
    pub headers: Vec<(String, String)>,
    pub auth: Option<String>,
}

impl Remote {
    /// `proxies` is what Python's `urllib.request.getproxies()` returns
    /// ({"https": url, "http": url, "no": hosts}), so requests reach the
    /// registry the way Ray's own do.
    pub fn new(
        timeout: Duration,
        headers: Vec<(String, String)>,
        auth: Option<String>,
        proxies: &HashMap<String, String>,
    ) -> Result<Remote, String> {
        let mut proxied = HashMap::new();
        for scheme in ["http", "https"] {
            if let Some(url) = proxies.get(scheme).filter(|url| !url.is_empty()) {
                let (address, authorization) = parse_proxy(url);
                let proxy = ureq::Proxy::new(address)
                    .map_err(|e| format!("unsupported {scheme} proxy {:?}: {e}", redact(url)))?;
                proxied.insert(scheme.to_string(), (agent(timeout, Some(proxy)), authorization));
            }
        }
        Ok(Remote {
            direct: agent(timeout, None),
            proxied,
            no_proxy: proxies.get("no").cloned(),
            headers,
            auth,
        })
    }

    /// The agent for `url`: its scheme's proxy unless `no_proxy` names its
    /// host. With it, the `Proxy-Authorization` value to send on a plain
    /// HTTP request; ureq sends a proxy's credentials only in the CONNECT
    /// that tunnels HTTPS.
    fn route(&self, url: &str) -> (&ureq::Agent, Option<&str>) {
        let (scheme, rest) = url.split_once("://").unwrap_or(("", url));
        let scheme = scheme.to_ascii_lowercase();
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        let host = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
        match self.proxied.get(&scheme) {
            Some((agent, authorization)) if !proxy_bypass(self.no_proxy.as_deref(), host) => {
                (agent, authorization.as_deref().filter(|_| scheme == "http"))
            }
            _ => (&self.direct, None),
        }
    }
}

/// `urllib.request.proxy_bypass_environment`: whether `host` ("name" or
/// "name:port") is in `no_proxy`, a comma-separated list of host names and
/// domain suffixes, or "*" for every host.
fn proxy_bypass(no_proxy: Option<&str>, host: &str) -> bool {
    let Some(no_proxy) = no_proxy else { return false };
    if no_proxy == "*" {
        return true;
    }
    let host = host.to_lowercase();
    // urllib's _splitport: a trailing ":<digits>" is the port.
    let hostonly = match host.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|c| c.is_ascii_digit()) => name,
        _ => host.as_str(),
    };
    for name in no_proxy.split(',') {
        let name = name.trim().trim_start_matches('.').to_lowercase();
        if name.is_empty() {
            continue;
        }
        if hostonly == name || host == name {
            return true;
        }
        let suffix = format!(".{name}");
        if hostonly.ends_with(&suffix) || host.ends_with(&suffix) {
            return true;
        }
    }
    false
}

/// A proxy URL as urllib reads it (`_parse_proxy`, `ProxyHandler`): only
/// the user, password and host:port count, the proxy is always spoken to in
/// plain HTTP (HTTPS goes through it with CONNECT), whatever scheme the URL
/// names, and the credentials are percent-decoded and used only if both are
/// there. Returns the proxy URL for ureq and the `Proxy-Authorization` value.
fn parse_proxy(url: &str) -> (String, Option<String>) {
    let rest = match url.split_once("://") {
        Some((scheme, rest)) if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https") => rest,
        // socks5://... and the like: let ureq refuse what it can't do.
        Some(_) => return (url.to_string(), None),
        None => url,
    };
    let end = match rest.find('@') {
        Some(at) => rest[at..].find('/').map(|i| at + i),
        None => rest.find('/'),
    };
    let authority = &rest[..end.unwrap_or(rest.len())];
    let Some((userinfo, hostport)) = authority.rsplit_once('@') else {
        return (format!("http://{authority}"), None);
    };
    match userinfo.split_once(':') {
        Some((user, password)) if !user.is_empty() && !password.is_empty() => {
            let (user, password) = (percent_decode(user), percent_decode(password));
            let authorization = format!("Basic {}", base64(format!("{user}:{password}").as_bytes()));
            (format!("http://{user}:{password}@{hostport}"), Some(authorization))
        }
        _ => (format!("http://{hostport}"), None),
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |c: u8| (c as char).to_digit(16);
        match (
            bytes[i],
            bytes.get(i + 1).copied().and_then(hex),
            bytes.get(i + 2).copied().and_then(hex),
        ) {
            (b'%', Some(hi), Some(lo)) => {
                out.push((hi * 16 + lo) as u8);
                i += 3;
            }
            (c, _, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn base64(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &b)| n | (b as u32) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// A proxy URL with any password hidden, for error messages.
fn redact(url: &str) -> String {
    match url.rsplit_once('@') {
        Some((_, host)) => format!("<credentials>@{host}"),
        None => url.to_string(),
    }
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
    let (agent, proxy_authorization) = remote.route(url);
    let mut req = agent.get(url);
    if let Some(value) = proxy_authorization {
        req = req.set("Proxy-Authorization", value);
    }
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

fn agent(timeout: Duration, proxy: Option<ureq::Proxy>) -> ureq::Agent {
    let mut builder = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(timeout)
        .timeout_read(timeout)
        .max_idle_connections_per_host(64);
    if let Some(proxy) = proxy {
        builder = builder.proxy(proxy);
    }
    builder.build()
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
    use super::{base64, digest_hex, join_url, parse_proxy, proxy_bypass};

    #[test]
    fn no_proxy_matches_as_urllib_does() {
        let no = Some("localhost, .internal.example,registry.corp:5000");
        for host in [
            "localhost",
            "localhost:8080",
            "a.internal.example",
            "internal.example",
            "REGISTRY.corp:5000",
        ] {
            assert!(proxy_bypass(no, host), "{host}");
        }
        for host in [
            "registry-1.docker.io",
            "notinternal.example",
            "registry.corp:5001",
            "example",
        ] {
            assert!(!proxy_bypass(no, host), "{host}");
        }
        assert!(proxy_bypass(Some("*"), "anything"));
        assert!(!proxy_bypass(None, "localhost"));
        assert!(!proxy_bypass(Some(""), "localhost"));
    }

    #[test]
    fn proxy_urls_read_as_urllib_reads_them() {
        let plain = |url: &str| parse_proxy(url).0;
        assert_eq!(plain("http://proxy:3128"), "http://proxy:3128");
        assert_eq!(plain("https://proxy:3128/"), "http://proxy:3128");
        assert_eq!(plain("proxy:3128"), "http://proxy:3128");
        assert_eq!(plain("socks5://proxy:1080"), "socks5://proxy:1080");
        // Credentials only when both are there, percent-decoded.
        assert_eq!(
            parse_proxy("http://user@proxy:3128"),
            ("http://proxy:3128".into(), None)
        );
        assert_eq!(
            parse_proxy("http://us%65r:p%40ss/w@proxy:3128/x"),
            (
                "http://user:p@ss/w@proxy:3128".into(),
                Some(format!("Basic {}", base64(b"user:p@ss/w")))
            )
        );
    }

    #[test]
    fn base64_matches_the_standard_alphabet() {
        assert_eq!(base64(b"user:pass"), "dXNlcjpwYXNz");
        assert_eq!(base64(b"a"), "YQ==");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(base64(b""), "");
    }

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
