//! The merged root filesystem, built the way Ray's extraction builds it.
//!
//! This mirrors `extract_tar_layer`, `apply_dir_attrs`, `_seed_tmp` and
//! `_seed_mountpoints` in `image_utils.py`, on an in-memory tree instead of a
//! directory: member paths resolve with symlinks followed inside the tree,
//! a directory entry over a symlink to a directory keeps the symlink, and so on.

use std::collections::{BTreeMap, VecDeque};

use crate::layer::{Entry, Kind};

pub enum NodeKind {
    Dir(BTreeMap<Vec<u8>, u32>),
    Reg { blk: u64, size: u64 },
    Sym(Vec<u8>),
}

pub struct Node {
    pub kind: NodeKind,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub mtime: u64,
}

pub const ROOT: u32 = 0;
const MAX_SYMLINK_HOPS: u32 = 40;

pub struct Tree {
    pub nodes: Vec<Node>,
    pub now: u64,
}

type Path = Vec<Vec<u8>>;

/// Components of a path, as `os.path.normpath` leaves them after the caller
/// has rejected `..` (empty and `.` components dropped).
fn components(p: &[u8]) -> Path {
    p.split(|&c| c == b'/')
        .filter(|c| !c.is_empty() && *c != b".")
        .map(|c| c.to_vec())
        .collect()
}

impl Tree {
    pub fn new(now: u64) -> Self {
        let mut t = Tree { nodes: Vec::new(), now };
        t.new_dir(0o755);
        t
    }

    fn add(&mut self, node: Node) -> u32 {
        self.nodes.push(node);
        (self.nodes.len() - 1) as u32
    }

    fn new_dir(&mut self, mode: u16) -> u32 {
        let now = self.now;
        self.add(Node {
            kind: NodeKind::Dir(BTreeMap::new()),
            mode,
            uid: 0,
            gid: 0,
            mtime: now,
        })
    }

    fn child(&self, dir: u32, name: &[u8]) -> Option<u32> {
        match &self.nodes[dir as usize].kind {
            NodeKind::Dir(m) => m.get(name).copied(),
            _ => None,
        }
    }

    fn children(&mut self, dir: u32) -> Option<&mut BTreeMap<Vec<u8>, u32>> {
        match &mut self.nodes[dir as usize].kind {
            NodeKind::Dir(m) => Some(m),
            _ => None,
        }
    }

    fn is_dir(&self, id: u32) -> bool {
        matches!(self.nodes[id as usize].kind, NodeKind::Dir(_))
    }

    /// The node at a canonical path, without following symlinks.
    fn lookup(&self, path: &[Vec<u8>]) -> Option<u32> {
        let mut cur = ROOT;
        for part in path {
            cur = self.child(cur, part)?;
        }
        Some(cur)
    }

    /// `_resolve_in_root`: follow every symlink with the tree as `/`; missing
    /// components stay as they are. `None` on too many hops (ELOOP).
    fn resolve(&self, path: &[Vec<u8>]) -> Option<Path> {
        let mut pending: VecDeque<Vec<u8>> = path.iter().cloned().collect();
        let mut resolved: Path = Vec::new();
        let mut cur = Some(ROOT);
        let mut hops = 0;
        while let Some(part) = pending.pop_front() {
            if part == b".." {
                resolved.pop();
                cur = self.lookup(&resolved);
                continue;
            }
            let next = cur.and_then(|c| self.child(c, &part));
            if let Some(NodeKind::Sym(target)) = next.map(|n| &self.nodes[n as usize].kind) {
                hops += 1;
                if hops > MAX_SYMLINK_HOPS {
                    return None;
                }
                if target.starts_with(b"/") {
                    resolved.clear();
                    cur = Some(ROOT);
                }
                for c in components(target).into_iter().rev() {
                    pending.push_front(c);
                }
                continue;
            }
            resolved.push(part);
            cur = next;
        }
        Some(resolved)
    }

    /// `os.makedirs(path, exist_ok=True)`: the directory at `path`, created
    /// (0755, root) where missing; `None` if a non-directory is in the way.
    fn makedirs(&mut self, path: &[Vec<u8>]) -> Option<u32> {
        let mut cur = ROOT;
        for part in path {
            cur = match self.child(cur, part) {
                Some(c) if self.is_dir(c) => c,
                Some(_) => return None,
                None => {
                    let d = self.new_dir(0o755);
                    self.children(cur)?.insert(part.clone(), d);
                    d
                }
            };
        }
        Some(cur)
    }

    pub fn apply(&mut self, e: &Entry) {
        let path = &e.path[e.path.iter().take_while(|&&c| c == b'/').count()..];
        if path.split(|&c| c == b'/').any(|c| c == b"..") {
            return;
        }
        let mut comps = components(path);
        let Some(base) = comps.pop() else {
            // The root itself: only its directory attributes apply.
            if let Kind::Dir = e.kind {
                let root = &mut self.nodes[ROOT as usize];
                if e.mode != 0 {
                    root.mode = e.mode;
                }
                root.mtime = e.mtime;
            }
            return;
        };
        let Some(parent) = self.resolve(&comps) else { return };
        let parent_id = self.lookup(&parent).filter(|&p| self.is_dir(p));

        if base == b".wh..wh..opq" {
            if let Some(p) = parent_id {
                self.children(p).unwrap().clear();
            }
            return;
        }
        if let Some(del) = base.strip_prefix(b".wh.") {
            if let (Some(p), false) = (parent_id, del.is_empty()) {
                self.children(p).unwrap().remove(del);
            }
            return;
        }

        if let Some(existing) = parent_id.and_then(|p| self.child(p, &base)) {
            let mut full = parent.clone();
            full.push(base.clone());
            let dir_behind = self
                .resolve(&full)
                .and_then(|r| self.lookup(&r))
                .is_some_and(|n| self.is_dir(n));
            if matches!(e.kind, Kind::Dir) && dir_behind {
                // An existing directory (or a symlink to one, as UsrMerge's
                // bin -> usr/bin) stays; a real directory takes the entry's
                // attributes, and either takes its owner.
                let node = &mut self.nodes[existing as usize];
                if matches!(node.kind, NodeKind::Dir(_)) {
                    if e.mode != 0 {
                        node.mode = e.mode;
                    }
                    node.mtime = e.mtime;
                }
                node.uid = e.uid;
                node.gid = e.gid;
                return;
            }
            self.children(parent_id.unwrap()).unwrap().remove(&base);
        }

        let kind = match &e.kind {
            Kind::Reg { blk, size } => NodeKind::Reg { blk: *blk, size: *size },
            Kind::Dir => NodeKind::Dir(BTreeMap::new()),
            Kind::Sym(target) => NodeKind::Sym(target.clone()),
            Kind::Hard(link) => {
                let link = &link[link.iter().take_while(|&&c| c == b'/').count()..];
                if link.split(|&c| c == b'/').any(|c| c == b"..") {
                    return;
                }
                let mut lcomps = components(link);
                let Some(lbase) = lcomps.pop() else { return };
                let Some(ldir) = self.resolve(&lcomps) else { return };
                let Some(target) = self.lookup(&ldir).and_then(|d| self.child(d, &lbase)) else {
                    return;
                };
                if self.is_dir(target) {
                    return;
                }
                let Some(p) = self.makedirs(&parent) else { return };
                self.children(p).unwrap().insert(base, target);
                return;
            }
            Kind::Other => return,
        };
        let mode = match (&kind, e.mode) {
            (NodeKind::Sym(_), _) => 0o777,
            (NodeKind::Dir(_), 0) => 0o755,
            (_, 0) => 0o644,
            (_, m) => m,
        };
        let Some(p) = self.makedirs(&parent) else { return };
        let id = self.add(Node {
            kind,
            mode,
            uid: e.uid,
            gid: e.gid,
            mtime: e.mtime,
        });
        self.children(p).unwrap().insert(base, id);
    }

    fn create_empty_file(&mut self, dir: u32, name: &[u8]) {
        if self.child(dir, name).is_none() {
            let now = self.now;
            let f = self.add(Node {
                kind: NodeKind::Reg { blk: 0, size: 0 },
                mode: 0o644,
                uid: 0,
                gid: 0,
                mtime: now,
            });
            self.children(dir).unwrap().insert(name.to_vec(), f);
        }
    }

    /// `_seed_tmp` then `_seed_mountpoints`.
    pub fn seed(&mut self) {
        if let Some(tmp) = self.resolve(&components(b"tmp")) {
            if let Some(d) = self.makedirs(&tmp) {
                self.nodes[d as usize].mode = 0o1777;
                self.create_empty_file(d, b".ray-sandbox-keep");
            }
        }
        let dirs: [&[u8]; 7] = [b"proc", b"sys", b"dev", b"dev/pts", b"dev/shm", b"run", b"tmp"];
        let files: [&[u8]; 3] = [b"etc/resolv.conf", b"etc/hosts", b"etc/hostname"];
        for (rel, is_dir) in dirs.iter().map(|d| (*d, true)).chain(files.iter().map(|f| (*f, false))) {
            let mut comps = components(rel);
            let base = comps.pop().unwrap();
            let Some(parent) = self.resolve(&comps) else { continue };
            if self.lookup(&parent).and_then(|p| self.child(p, &base)).is_some() {
                continue;
            }
            let Some(p) = self.makedirs(&parent) else { continue };
            if is_dir {
                let d = self.new_dir(0o755);
                self.children(p).unwrap().insert(base, d);
            } else {
                self.create_empty_file(p, &base);
            }
        }
    }
}
