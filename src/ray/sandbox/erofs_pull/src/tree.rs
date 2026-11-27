//! The merged root filesystem, built the way Ray's extraction builds it.
//!
//! This mirrors `extract_tar_layer`, `apply_dir_attrs` and `_seed_tmp` in
//! `image_utils.py`, on an in-memory tree instead of a
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
/// The longest name EROFS (EROFS_NAME_LEN) and Linux (NAME_MAX) take.
pub const MAX_NAME: usize = 255;
const MAX_SYMLINK_HOPS: u32 = 40;

pub struct Tree {
    pub nodes: Vec<Node>,
    /// The mtime of entries the build creates (missing parents, seeds).
    pub epoch: u64,
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
    pub fn new(epoch: u64) -> Self {
        let mut t = Tree {
            nodes: Vec::new(),
            epoch,
        };
        t.new_dir(0o755);
        t
    }

    fn add(&mut self, node: Node) -> u32 {
        self.nodes.push(node);
        (self.nodes.len() - 1) as u32
    }

    fn new_dir(&mut self, mode: u16) -> u32 {
        let epoch = self.epoch;
        self.add(Node {
            kind: NodeKind::Dir(BTreeMap::new()),
            mode,
            uid: 0,
            gid: 0,
            mtime: epoch,
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
    /// (0755, root) where missing; `None` if a non-directory is in the way or
    /// a missing name is longer than `MAX_NAME` (ENAMETOOLONG), the
    /// directories before it staying created, as `os.makedirs` leaves them.
    fn makedirs(&mut self, path: &[Vec<u8>]) -> Option<u32> {
        let mut cur = ROOT;
        for part in path {
            cur = match self.child(cur, part) {
                Some(c) if self.is_dir(c) => c,
                Some(_) => return None,
                None if part.len() > MAX_NAME => return None,
                None => {
                    let d = self.new_dir(0o755);
                    self.children(cur)?.insert(part.clone(), d);
                    d
                }
            };
        }
        Some(cur)
    }

    /// Applies a layer's entry. Fails where extraction does: on a name longer
    /// than `MAX_NAME` for a parent directory, file or directory it would
    /// create (`os.makedirs` and `os.open` raise ENAMETOOLONG), and on a
    /// parent that isn't a directory (`os.makedirs` raises); a symlink or
    /// hard link of a long name is skipped, as extraction ignores the error
    /// there.
    pub fn apply(&mut self, e: &Entry) -> Result<(), String> {
        let too_long = || {
            format!(
                "a name in {:?} is longer than {MAX_NAME} bytes",
                String::from_utf8_lossy(&e.path)
            )
        };
        // makedirs fails on a parent that's a file (or a symlink to one):
        // extraction's os.makedirs raises there too, and the pull fails.
        let not_a_directory = || format!("a parent of {:?} isn't a directory", String::from_utf8_lossy(&e.path));
        let path = &e.path[e.path.iter().take_while(|&&c| c == b'/').count()..];
        if path.split(|&c| c == b'/').any(|c| c == b"..") {
            return Ok(());
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
            return Ok(());
        };
        let Some(parent) = self.resolve(&comps) else {
            return Ok(());
        };
        let parent_id = self.lookup(&parent).filter(|&p| self.is_dir(p));

        if base == b".wh..wh..opq" {
            if let Some(p) = parent_id {
                self.children(p).unwrap().clear();
            }
            return Ok(());
        }
        if let Some(del) = base.strip_prefix(b".wh.") {
            if let (Some(p), false) = (parent_id, del.is_empty()) {
                self.children(p).unwrap().remove(del);
            }
            return Ok(());
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
                return Ok(());
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
                    return Ok(());
                }
                let mut lcomps = components(link);
                let Some(lbase) = lcomps.pop() else { return Ok(()) };
                let Some(ldir) = self.resolve(&lcomps) else {
                    return Ok(());
                };
                let Some(target) = self.lookup(&ldir).and_then(|d| self.child(d, &lbase)) else {
                    return Ok(());
                };
                if self.is_dir(target) {
                    return Ok(());
                }
                if parent.iter().any(|c| c.len() > MAX_NAME) {
                    return Err(too_long());
                }
                if base.len() > MAX_NAME {
                    return Ok(());
                }
                let Some(p) = self.makedirs(&parent) else {
                    return Err(not_a_directory());
                };
                self.children(p).unwrap().insert(base, target);
                return Ok(());
            }
            Kind::Other => return Ok(()),
        };
        let mode = match (&kind, e.mode) {
            (NodeKind::Sym(_), _) => 0o777,
            (NodeKind::Dir(_), 0) => 0o755,
            (_, 0) => 0o644,
            (_, m) => m,
        };
        if parent.iter().any(|c| c.len() > MAX_NAME) {
            return Err(too_long());
        }
        if base.len() > MAX_NAME {
            return match kind {
                NodeKind::Sym(_) => Ok(()),
                _ => Err(too_long()),
            };
        }
        let Some(p) = self.makedirs(&parent) else {
            return Err(not_a_directory());
        };
        let id = self.add(Node {
            kind,
            mode,
            uid: e.uid,
            gid: e.gid,
            mtime: e.mtime,
        });
        self.children(p).unwrap().insert(base, id);
        Ok(())
    }

    fn create_empty_file(&mut self, dir: u32, name: &[u8]) {
        if self.child(dir, name).is_none() {
            let epoch = self.epoch;
            let f = self.add(Node {
                kind: NodeKind::Reg { blk: 0, size: 0 },
                mode: 0o644,
                uid: 0,
                gid: 0,
                mtime: epoch,
            });
            self.children(dir).unwrap().insert(name.to_vec(), f);
        }
    }

    /// `_seed_tmp`. A seed whose path can't be created (a file in the way,
    /// a name longer than `MAX_NAME` through a symlink) is skipped, as its
    /// `except OSError` skips it.
    pub fn seed(&mut self) {
        if let Some(tmp) = self.resolve(&components(b"tmp")) {
            if let Some(d) = self.makedirs(&tmp) {
                self.nodes[d as usize].mode = 0o1777;
                self.create_empty_file(d, b".ray-sandbox-keep");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NodeKind, Tree, MAX_NAME, ROOT};
    use crate::layer::{Entry, Kind};

    fn entry(path: Vec<u8>, kind: Kind) -> Entry {
        Entry {
            path,
            kind,
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime: 0,
        }
    }

    fn names(tree: &Tree) -> Vec<Vec<u8>> {
        match &tree.nodes[ROOT as usize].kind {
            NodeKind::Dir(children) => children.keys().cloned().collect(),
            _ => unreachable!(),
        }
    }

    #[test]
    fn names_longer_than_erofs_takes_fail_as_extraction_does() {
        let long = vec![b'x'; MAX_NAME + 1];
        let reg = || Kind::Reg { blk: 0, size: 0 };
        let mut tree = Tree::new(0);
        // A name of MAX_NAME bytes is fine.
        tree.apply(&entry(vec![b'y'; MAX_NAME], reg())).unwrap();
        // A longer file, directory, or parent of anything: an error.
        assert!(tree.apply(&entry(long.clone(), reg())).is_err());
        assert!(tree.apply(&entry(long.clone(), Kind::Dir)).is_err());
        let under_long = [long.as_slice(), b"/f"].concat();
        assert!(tree.apply(&entry(under_long.clone(), reg())).is_err());
        assert!(tree.apply(&entry(under_long, Kind::Sym(b"t".to_vec()))).is_err());
        // A longer symlink is skipped, as extraction ignores its failure.
        tree.apply(&entry(long.clone(), Kind::Sym(b"t".to_vec()))).unwrap();
        // A whiteout of a long name creates nothing.
        tree.apply(&entry([b".wh.".as_slice(), &long].concat(), reg())).unwrap();
        assert_eq!(names(&tree), vec![vec![b'y'; MAX_NAME]]);
    }

    /// Every name in the tree, at any depth.
    fn all_names(tree: &Tree, dir: u32, out: &mut Vec<Vec<u8>>) {
        if let NodeKind::Dir(children) = &tree.nodes[dir as usize].kind {
            for (name, &child) in children {
                out.push(name.clone());
                if child != dir {
                    all_names(tree, child, out);
                }
            }
        }
    }

    #[test]
    fn a_file_in_the_way_of_a_parent_fails_as_extraction_does() {
        let reg = || Kind::Reg { blk: 0, size: 0 };
        let mut tree = Tree::new(0);
        tree.apply(&entry(b"app".to_vec(), reg())).unwrap();
        tree.apply(&entry(b"target".to_vec(), reg())).unwrap();
        for kind in [reg(), Kind::Sym(b"x".to_vec()), Kind::Hard(b"target".to_vec())] {
            let err = tree.apply(&entry(b"app/config.yaml".to_vec(), kind)).unwrap_err();
            assert!(err.contains("isn't a directory"), "{err}");
        }
        // The seed still skips what's in its way, as extraction's does.
        tree.apply(&entry(b"tmp".to_vec(), reg())).unwrap();
        tree.seed();
    }

    #[test]
    fn seeds_through_a_symlink_to_a_long_name_are_skipped() {
        let long = "x".repeat(MAX_NAME + 1);
        for target in [format!("/{long}"), format!("/a/{long}/b")] {
            let mut tree = Tree::new(0);
            tree.apply(&entry(b"tmp".to_vec(), Kind::Sym(target.clone().into_bytes())))
                .unwrap();
            tree.seed();
            let mut names = Vec::new();
            all_names(&tree, ROOT, &mut names);
            assert!(names.iter().all(|n| n.len() <= MAX_NAME), "{target}");
            // As os.makedirs does, what comes before the long name is created.
            assert_eq!(names.contains(&b"a".to_vec()), target.starts_with("/a/"), "{target}");
        }
    }
}
