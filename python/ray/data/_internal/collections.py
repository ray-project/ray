from typing import Dict, TypeVar

K = TypeVar("K")


def collapse_transitive_map(d: Dict[K, K]) -> Dict[K, K]:
    """Collapse transitive mappings in a dictionary. Given a mapping like
    {a: b, b: c, c: d}, returns {a: d}, removing intermediate b -> c, c -> d.

    Only keeps mappings where the key is NOT a value in another mapping (i.e., chain starting points).

    Args:
        d: Dictionary representing a mapping

    Returns:
        Dictionary with all transitive mappings collapsed, keeping only KV-pairs,
        such that K and V are starting and terminal point of a chain

    Examples:
        >>> collapse_transitive_map({"a": "b", "b": "c", "c": "d"})
        {'a': 'd'}
        >>> collapse_transitive_map({"a": "b", "x": "y"})
        {'a': 'b', 'x': 'y'}
    """
    if not d:
        return {}

    collapsed = {}
    values_set = set(d.values())
    for k in d:
        # Skip mappings that are in the value-set, meaning that they are
        # part of the mapping chain (for ex, {a -> b, b -> c})
        if k in values_set:
            continue

        cur = k
        visited = {cur}

        # Follow the chain until we reach a key that's not in the mapping
        while cur in d:
            next = d[cur]
            if next in visited:
                raise ValueError(f"Detected a cycle in the mapping {d}")
            visited.add(next)
            cur = next

        collapsed[k] = cur

    return collapsed
