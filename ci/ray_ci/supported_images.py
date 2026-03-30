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
    """
    Format platform as -cpu, -tpu, or shortened CUDA version.

    Examples:
        cpu -> -cpu
        tpu -> -tpu
        cu12.1.1-cudnn8 -> -cu121
        cu12.3.2-cudnn9 -> -cu123
    """
    if platform == "cpu":
        return "-cpu"
    if platform == "tpu":
        return "-tpu"
    # cu12.3.2-cudnn9 -> -cu123
    platform_base = platform.split("-", 1)[0]
    parts = platform_base.split(".")
    if len(parts) < 2:
        raise ValueError(f"Unrecognized GPU platform format: {platform}")
    return f"-{parts[0]}{parts[1]}"


def build_platform_reverse_map() -> Dict[str, str]:
    """
    Build a reverse map from short platform tag to full platform string.

    Uses format_platform_tag on every platform across all image types in
    ray-images.json.  The returned dict maps the short form (without leading
    hyphen, e.g. "cu123") to the full platform string (e.g.
    "cu12.3.2-cudnn9").
    """
    reverse_map: Dict[str, str] = {}
    images = load_supported_images()
    for image_type_config in images.values():
        for platform in image_type_config.get("platforms", []):
            short = format_platform_tag(platform).lstrip("-")
            if short in reverse_map:
                if reverse_map[short] != platform:
                    raise ValueError(
                        f"Ambiguous short platform tag '{short}': "
                        f"could be '{platform}' or '{reverse_map[short]}'"
                    )
            else:
                reverse_map[short] = platform
    return reverse_map
