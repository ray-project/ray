import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List

_RAYCI_VERSION_FILE = ".rayciversion"


def _find_ray_root() -> Path:
    """Walk up from this file and cwd looking for .rayciversion."""
    start = Path(__file__).resolve()
    for parent in start.parents:
        if (parent / _RAYCI_VERSION_FILE).exists():
            return parent
    if (Path.cwd() / _RAYCI_VERSION_FILE).exists():
        return Path.cwd()
    raise FileNotFoundError("Could not find Ray root (missing .rayciversion).")


@lru_cache(maxsize=1)
def load_supported_images():
    path = _find_ray_root() / "ray-images.json"
    return json.loads(path.read_text())


def get_image_config(image_type: str) -> dict:
    return load_supported_images()[image_type]


def get_python_versions(image_type: str) -> List[str]:
    return get_image_config(image_type)["python"]


def get_platforms(image_type: str) -> List[str]:
    return get_image_config(image_type)["platforms"]


def get_architectures(image_type: str) -> List[str]:
    return get_image_config(image_type)["architectures"]


def get_exceptions(image_type: str) -> List[dict]:
    return get_image_config(image_type).get("exceptions", [])


def get_default(image_type: str, key: str) -> str:
    return get_image_config(image_type)["defaults"][key]


def format_platform_tag(platform: str) -> str:
    """Format a full platform string as a short tag suffix.

    Examples:
        cpu -> -cpu
        tpu -> -tpu
        cu12.3.2-cudnn9 -> -cu123
        cu13.0.0-cudnn -> -cu130
    """
    if platform in ("cpu", "tpu"):
        return f"-{platform}"
    # cu12.3.2-cudnn9 -> cu12.3.2 -> ["cu12", "3", "2"] -> -cu123
    platform_base = platform.split("-", 1)[0]
    parts = platform_base.split(".")
    if len(parts) < 2:
        raise ValueError(f"Unrecognized GPU platform format: {platform}")
    return f"-{parts[0]}{parts[1]}"


def build_platform_reverse_map() -> Dict[str, str]:
    """Build a reverse map from short tag suffix to full platform string.

    Reads ray-images.json to get all valid platforms across image types,
    then uses format_platform_tag() to build the reverse mapping.

    Returns:
        Dict mapping short suffix (e.g., "cu123") to full platform
        (e.g., "cu12.3.2-cudnn9").
    """
    reverse_map = {}
    for image_type in ("ray", "ray-ml", "ray-llm"):
        for platform in get_platforms(image_type):
            short = format_platform_tag(platform).lstrip("-")
            reverse_map[short] = platform
    return reverse_map
