import tree

from typing import Any, Dict


def flatten_dict(nested: Dict[str, Any], sep="/", env_steps=0) -> Dict[str, Any]:
    """
    Flattens a nested dict into a flat dict with joined keys.

    Note, this is used for better serialization of nested dictionaries
    in `OfflinePreLearner.__call__` when called inside
    `ray.data.Dataset.map_batches`.

    Note, this is used to return a `Dict[str, numpy.ndarray] from the
    `__call__` method which is expected by Ray Data.

    Args:
        nested: A nested dictionary.
        sep: Separator to use when joining keys.

    Returns:
        A flat dictionary where each key is a path of keys in the nested dict.
    """
    flat = {}
    # `dm_tree.flatten_with_path`` returns a list of `(path, leaf)` tuples.
    for path, leaf in tree.flatten_with_path(nested):
        # Create a single string key from the path.
        key = sep.join(str(p) for p in path)
        flat[key] = leaf

    return flat


def unflatten_dict(flat: Dict[str, Any], sep="/") -> Dict[str, Any]:
    """
    Reconstructs a nested dict from a flat dict with joined keys.

    Note, this is used for better deserialization ofr nested dictionaries
    in `Learner.update' calls in which a `ray.data.DataIterator` is used.

    Args:
        flat: A flat dictionary with keys that are paths joined by `sep`.
        sep: The separator used in the flat dictionary keys.

    Returns:
        A nested dictionary.
    """
    nested = {}
    for compound_key, value in flat.items():
        # Split all keys by the separator.
        keys = compound_key.split(sep)
        current = nested
        # Nest by the separated keys.
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value

    return nested
