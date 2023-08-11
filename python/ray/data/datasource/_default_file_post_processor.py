from typing import Any, Dict, List, Tuple

def post_process_files(
    paths: List[str],
    file_sizes: List[int],
    reader_args: Dict[str, Any]
) -> Tuple[List[str], List[int]]:
    # Don't do anything by default.
    return (paths, file_sizes)