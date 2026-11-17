//! Writes the tree's metadata as an uncompressed EROFS image.
//!
//! Layout: block 0 holds the superblock; file data already sits in the data
//! blocks the layers wrote; directory and symlink data follow it; the inode
//! table comes last. Every inode is a 64-byte extended inode with the
//! flat-plain data layout, which gVisor's reader supports (no inline data,
//! no xattrs, no compression). Blocks are the host's page size. The root is inode 0, as its nid must fit the
//! superblock's 16-bit field.

use std::io;
use std::os::unix::fs::FileExt;

use crate::layer::Out;
use crate::tree::{NodeKind, Tree, MAX_NAME, ROOT};

const MAGIC: u32 = 0xe0f5e1e2;
const INODE_SIZE: u64 = 64;
const FT_REG: u8 = 1;
const FT_DIR: u8 = 2;
const FT_SYMLINK: u8 = 7;

fn file_type(kind: &NodeKind) -> u8 {
    match kind {
        NodeKind::Dir(_) => FT_DIR,
        NodeKind::Reg { .. } => FT_REG,
        NodeKind::Sym(_) => FT_SYMLINK,
    }
}

/// Directory data: blocks of 12-byte dirents followed by their names, the
/// entries sorted by name across blocks (lookups binary-search them).
fn dir_blocks(mut entries: Vec<(&[u8], u64, u8)>, block: usize) -> io::Result<Vec<u8>> {
    // EROFS takes names of at most MAX_NAME bytes. Tree keeps to that; this
    // catches any path that doesn't.
    if let Some((name, _, _)) = entries.iter().find(|(name, _, _)| name.len() > MAX_NAME) {
        return Err(io::Error::other(format!(
            "a directory entry is longer than {MAX_NAME} bytes: {:?}",
            String::from_utf8_lossy(name)
        )));
    }
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut data = Vec::new();
    let mut i = 0;
    while i < entries.len() {
        // As many entries as fit in this block.
        let mut n = 0;
        let mut used = 0;
        while i + n < entries.len() && used + 12 + entries[i + n].0.len() <= block {
            used += 12 + entries[i + n].0.len();
            n += 1;
        }
        if n == 0 {
            // Unreachable: a name of MAX_NAME bytes always fits a block.
            return Err(io::Error::other("a directory entry doesn't fit in a block"));
        }
        let block_start = data.len();
        let mut name_off = 12 * n;
        for (name, nid, ft) in &entries[i..i + n] {
            data.extend_from_slice(&(*nid as u32).to_le_bytes());
            data.extend_from_slice(&((*nid >> 32) as u32).to_le_bytes());
            data.extend_from_slice(&(name_off as u16).to_le_bytes());
            data.push(*ft);
            data.push(0);
            name_off += name.len();
        }
        for (name, _, _) in &entries[i..i + n] {
            data.extend_from_slice(name);
        }
        i += n;
        if i < entries.len() {
            data.resize(block_start + block, 0);
        }
    }
    Ok(data)
}

pub fn write(tree: &Tree, out: &Out) -> io::Result<u64> {
    // Number the inodes breadth-first from the root, once each (hard links
    // share one), and count the names that refer to each.
    let mut index = vec![u32::MAX; tree.nodes.len()];
    let mut order = vec![ROOT];
    let mut parent = vec![ROOT; tree.nodes.len()];
    let mut nlink = vec![0u32; tree.nodes.len()];
    index[ROOT as usize] = 0;
    let mut i = 0;
    while i < order.len() {
        let id = order[i];
        if let NodeKind::Dir(children) = &tree.nodes[id as usize].kind {
            nlink[id as usize] += 2;
            for &c in children.values() {
                if tree.nodes[c as usize].kind_is_dir() {
                    nlink[id as usize] += 1;
                    parent[c as usize] = id;
                } else {
                    nlink[c as usize] += 1;
                }
                if index[c as usize] == u32::MAX {
                    index[c as usize] = order.len() as u32;
                    order.push(c);
                }
            }
        }
        i += 1;
    }
    let nid = |id: u32| index[id as usize] as u64 * (INODE_SIZE / 32);

    // Directory and symlink data, after the file data.
    let mut inode_table = vec![0u8; order.len() * INODE_SIZE as usize];
    for (k, &id) in order.iter().enumerate() {
        let node = &tree.nodes[id as usize];
        let (fmt, size, blk) = match &node.kind {
            NodeKind::Reg { blk, size } => (0o100000u16, *size, *blk),
            NodeKind::Sym(target) => {
                let blk = out.alloc(target.len() as u64);
                out.file.write_all_at(target, blk * out.block)?;
                (0o120000, target.len() as u64, blk)
            }
            NodeKind::Dir(children) => {
                let mut entries: Vec<(&[u8], u64, u8)> =
                    vec![(b".", nid(id), FT_DIR), (b"..", nid(parent[id as usize]), FT_DIR)];
                for (name, &c) in children {
                    entries.push((name, nid(c), file_type(&tree.nodes[c as usize].kind)));
                }
                let data = dir_blocks(entries, out.block as usize)?;
                let blk = out.alloc(data.len() as u64);
                out.file.write_all_at(&data, blk * out.block)?;
                (0o040000, data.len() as u64, blk)
            }
        };
        let ino = &mut inode_table[k * INODE_SIZE as usize..(k + 1) * INODE_SIZE as usize];
        ino[0..2].copy_from_slice(&1u16.to_le_bytes()); // extended, flat plain
        ino[4..6].copy_from_slice(&(fmt | node.mode).to_le_bytes());
        ino[8..16].copy_from_slice(&size.to_le_bytes());
        ino[16..20].copy_from_slice(&(blk as u32).to_le_bytes());
        ino[20..24].copy_from_slice(&(k as u32 + 1).to_le_bytes());
        ino[24..28].copy_from_slice(&node.uid.to_le_bytes());
        ino[28..32].copy_from_slice(&node.gid.to_le_bytes());
        ino[32..40].copy_from_slice(&node.mtime.to_le_bytes());
        ino[44..48].copy_from_slice(&nlink[id as usize].to_le_bytes());
    }

    let meta_blk = out.alloc(inode_table.len() as u64);
    out.file.write_all_at(&inode_table, meta_blk * out.block)?;
    let blocks = out.blocks();
    if blocks > u32::MAX as u64 {
        return Err(io::Error::other("image too large for 32-bit block addresses"));
    }

    let mut sb = [0u8; 128];
    sb[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    sb[12] = out.block.trailing_zeros() as u8;
    sb[14..16].copy_from_slice(&0u16.to_le_bytes()); // root nid
    sb[16..24].copy_from_slice(&(order.len() as u64).to_le_bytes());
    // Build time 0 and a zero UUID, as `mkfs.erofs -T0 --mkfs-time
    // -U00000000-0000-0000-0000-000000000000` writes: gVisor restores a
    // checkpoint only onto an image with the superblock it was taken on.
    sb[24..32].copy_from_slice(&0u64.to_le_bytes());
    sb[36..40].copy_from_slice(&(blocks as u32).to_le_bytes());
    sb[40..44].copy_from_slice(&(meta_blk as u32).to_le_bytes());
    out.file.write_all_at(&sb, 1024)?;
    out.file.set_len(blocks * out.block)?;
    Ok(order.len() as u64)
}

impl crate::tree::Node {
    fn kind_is_dir(&self) -> bool {
        matches!(self.kind, NodeKind::Dir(_))
    }
}

#[cfg(test)]
mod tests {
    use super::dir_blocks;

    #[test]
    fn names_longer_than_erofs_takes_are_an_error_not_a_hang() {
        for len in [256, 4000, 5000] {
            let long = vec![b'x'; len];
            assert!(dir_blocks(vec![(&long, 1, 1)], 4096).is_err(), "{len}");
        }
        let name = vec![b'x'; 254]; // 255 bytes with the suffix below
        let names: Vec<_> = (0..40).map(|i| [name.as_slice(), &[i as u8]].concat()).collect();
        let data = dir_blocks(names.iter().map(|n| (n.as_slice(), 1, 1)).collect(), 4096).unwrap();
        assert!(data.len() > 4096);
    }
}
