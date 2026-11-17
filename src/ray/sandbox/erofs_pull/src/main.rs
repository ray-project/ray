//! erofs-pull: build a gVisor-ready EROFS image straight from registry layers.
//!
//! Reads a JSON spec on stdin (the caller resolves the manifest and auth):
//!   {"base_url": ".../v2/<repo>/blobs/", "headers": {..}, "auth": "Bearer ..",
//!    "layers": [{"digest": "sha256:..", "size": N}, ..],
//!    "blob_dir": "<images_dir>/.blobs/sha256", "out": "rootfs.erofs",
//!    "block_size": 4096, "connections": 16, "chunk_mib": 16,
//!    "timeout_seconds": 120}
//!
//! Every layer downloads over ranged requests and is decompressed, hashed and
//! parsed while it arrives, all layers at once; file data is written once,
//! into the output image. The layers are then applied in order to an
//! in-memory tree and the metadata is written. Timings go to stderr as JSON.

mod erofs;
mod fetch;
mod layer;
mod tree;

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use fetch::{Blob, BlobReader, Remote};
use layer::Out;

#[derive(Deserialize)]
struct LayerSpec {
    digest: String,
    size: u64,
}

#[derive(Deserialize)]
struct Spec {
    base_url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    auth: Option<String>,
    layers: Vec<LayerSpec>,
    blob_dir: String,
    out: String,
    #[serde(default = "default_connections")]
    connections: usize,
    #[serde(default = "default_chunk_mib")]
    chunk_mib: u64,
    /// Image block size: the host's page size (4096, or 65536 on 64K-page
    /// kernels), which gVisor requires.
    #[serde(default = "default_block_size")]
    block_size: u64,
    /// rapidgzip threads per layer; 0 for all cores.
    #[serde(default)]
    gzip_threads: u32,
    #[serde(default = "default_timeout")]
    timeout_seconds: f64,
}

fn default_connections() -> usize {
    16
}
fn default_chunk_mib() -> u64 {
    16
}
fn default_block_size() -> u64 {
    4096
}
fn default_timeout() -> f64 {
    120.0
}

fn hash_blob(blob: Arc<Blob>) -> io::Result<()> {
    let mut reader = BlobReader::new(blob.clone());
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let got = format!("sha256:{:x}", hasher.finalize());
    if got != blob.digest {
        return Err(io::Error::other(format!("layer {} has digest {got}", blob.digest)));
    }
    Ok(())
}

fn run(spec: Spec) -> Result<(), String> {
    let t0 = Instant::now();
    let ms = |t: Instant| t.duration_since(t0).as_millis() as u64;
    let remote = Arc::new(Remote {
        agent: fetch::agent(Duration::from_secs_f64(spec.timeout_seconds)),
        headers: spec.headers.into_iter().collect(),
        auth: spec.auth,
    });
    // An image may list a layer more than once: each distinct blob is opened
    // (and locked, downloaded and hashed) once, and every layer that uses it
    // reads it.
    let mut unique: Vec<Arc<Blob>> = Vec::new();
    let mut blobs: Vec<Arc<Blob>> = Vec::new();
    for l in &spec.layers {
        let blob = match unique.iter().find(|b| b.digest == l.digest) {
            Some(b) => b.clone(),
            None => {
                let b = Blob::open(&spec.blob_dir, &spec.base_url, &l.digest, l.size, spec.chunk_mib << 20)
                    .map_err(|e| e.to_string())?;
                unique.push(b.clone());
                b
            }
        };
        blobs.push(blob);
    }
    let out_file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&spec.out)
        .map_err(|e| e.to_string())?;
    if !spec.block_size.is_power_of_two() || !(4096..=65536).contains(&spec.block_size) {
        return Err(format!(
            "block_size {} is not a power of two from 4096 to 65536",
            spec.block_size
        ));
    }
    let out = Arc::new(Out::new(out_file, spec.block_size));

    let downloads = fetch::download_all(remote, &unique, spec.connections);
    let hashes: Vec<_> = unique
        .iter()
        .map(|b| {
            let b = b.clone();
            (!b.cached()).then(|| std::thread::spawn(move || hash_blob(b).map(|_| Instant::now())))
        })
        .collect();
    let workers: Vec<_> = blobs
        .iter()
        .map(|b| {
            let (b2, out) = (b.clone(), out.clone());
            let threads = spec.gzip_threads;
            let build = std::thread::spawn(move || {
                layer::read_layer(BlobReader::new(b2), threads, &out).map(|e| (e, Instant::now()))
            });
            build
        })
        .collect();

    let mut error = None;
    let mut hashed = Vec::new();
    for (hash, blob) in hashes.into_iter().zip(&unique) {
        match hash.map(|h| h.join().unwrap()).transpose() {
            Ok(t) => hashed.push((blob.digest.clone(), t)),
            Err(e) => {
                error.get_or_insert(format!("{}: {e}", blob.digest));
            }
        }
    }
    let mut layers = Vec::new();
    let mut timings = Vec::new();
    for (build, blob) in workers.into_iter().zip(&blobs) {
        let h = Ok(hashed.iter().find(|(d, _)| *d == blob.digest).and_then(|(_, t)| *t));
        match (h, build.join().unwrap()) {
            (Ok(h), Ok((entries, t))) => {
                timings.push(serde_json::json!({
                    "digest": blob.digest, "size": blob.size, "cached": blob.cached(),
                    "downloaded_ms": h.map(ms), "built_ms": ms(t), "entries": entries.len(),
                }));
                layers.push(entries);
            }
            (Err::<_, io::Error>(e), _) | (_, Err(e)) => {
                error.get_or_insert(format!("{}: {e}", blob.digest));
            }
        }
    }
    for d in downloads {
        d.join().unwrap();
    }
    if let Some(e) = error {
        unique.iter().for_each(|b| b.discard());
        let _ = std::fs::remove_file(&spec.out);
        return Err(e);
    }
    for b in &unique {
        b.commit().map_err(|e| e.to_string())?;
    }
    let t_layers = Instant::now();

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let mut tree = tree::Tree::new(now);
    for entries in &layers {
        for e in entries {
            tree.apply(e);
        }
    }
    tree.seed();
    let t_tree = Instant::now();
    let inodes = erofs::write(&tree, &out).map_err(|e| e.to_string())?;
    let t_done = Instant::now();
    eprintln!(
        "{}",
        serde_json::json!({
            "layers": timings, "layers_ms": ms(t_layers), "tree_ms": t_tree.duration_since(t_layers).as_millis() as u64,
            "metadata_ms": t_done.duration_since(t_tree).as_millis() as u64, "total_ms": ms(t_done),
            "inodes": inodes, "image_bytes": out.blocks() * out.block,
        })
    );
    Ok(())
}

fn main() {
    let spec: Spec = match serde_json::from_reader(io::stdin()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("erofs-pull: bad spec: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = run(spec) {
        eprintln!("erofs-pull: {e}");
        std::process::exit(1);
    }
}
