# rllib/utils/env_vars.py (or rllib/env_config.py)
"""Environment variables respected by RLlib.

Users can override these to point to custom data locations, mirrors, etc.
"""
import os


def _get_env(var: str, default: str) -> str:
    """Get env var, normalizing trailing slashes for URLs/paths."""
    value = os.environ.get(var, default)
    return value.rstrip("/") + "/" if value else default


# ---------------------
# Example/Public Data
# ---------------------
RLLIB_EXAMPLE_DATA_S3_ROOT = _get_env(
    "RLLIB_EXAMPLE_DATA_S3_ROOT",
    "https://ray-example-data.s3.us-west-2.amazonaws.com/rllib/",
)

# ---------------------
# Offline RL Datasets
# ---------------------
RLLIB_OFFLINE_DATA_S3_ROOT = _get_env(
    "RLLIB_OFFLINE_DATA_S3_ROOT", RLLIB_EXAMPLE_DATA_S3_ROOT + "offline-data/"
)

# ---------------------
# Environment Binaries
# ---------------------
RLLIB_FOOTSIES_BINARIES_URL = _get_env(
    "RLLIB_FOOTSIES_BINARIES_URL", RLLIB_EXAMPLE_DATA_S3_ROOT + "env-footsies/binaries/"
)
