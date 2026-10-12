import logging
from typing import Optional, Set

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


def find_owning_checkpoint_id(
    file_name: str, checkpoint_ids: Set[str]
) -> Optional[str]:
    """Find the checkpoint ID of the write task that wrote a data file.

    Every data file a write task writes starts with that task's checkpoint ID.
    One task's ID can also be a prefix of another task's ID: task indices are
    padded to 6 digits, so task 100000's ID ("..._100000") is a prefix of task
    1000000's ID ("..._1000000") and of its file names. The longest matching
    ID is the task that wrote the file.

    Args:
        file_name: The basename of a data file.
        checkpoint_ids: The IDs of all committed and pending checkpoints.

    Returns:
        The longest ID in `checkpoint_ids` that is a prefix of `file_name`, or
        None if no ID is a prefix.
    """
    for end in range(len(file_name), 0, -1):
        if file_name[:end] in checkpoint_ids:
            return file_name[:end]
    return None
