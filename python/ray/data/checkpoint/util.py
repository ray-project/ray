import logging
import posixpath
from typing import List

logger = logging.getLogger(__name__)


class PrefixTrie:
    """Trie for efficient prefix matching of filenames during recovery."""

    def __init__(self) -> None:
        self.children: dict[str, "PrefixTrie"] = {}
        self.is_end: bool = False

    def insert(self, word: str) -> None:
        node = self
        for ch in word:
            if ch not in node.children:
                node.children[ch] = PrefixTrie()
            node = node.children[ch]
        node.is_end = True

    def has_prefix_of(self, word: str) -> bool:
        """Return True if any inserted word is a prefix of `word`."""
        node = self
        for ch in word:
            if node.is_end:
                return True
            if ch not in node.children:
                return False
            node = node.children[ch]
        return node.is_end


def build_pending_checkpoint_trie(file_paths: List, pending_suffix: str) -> PrefixTrie:
    """Build a PrefixTrie from pending checkpoint file paths.

    Strips the given pending suffix to get the data file prefix.
    """
    trie = PrefixTrie()
    for f in file_paths:
        basename = posixpath.basename(f.path)
        if basename.endswith(pending_suffix):
            prefix = basename[: -len(pending_suffix)]
            trie.insert(prefix)
    return trie
