import os
from functools import cache

import yaml

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
# We default to the OPEN_API_BASE for per-env settings differentiation
OVERRIDE_KEY = os.getenv("PROBES_OVERRIDE_KEY", os.getenv("OPENAI_API_BASE"))


@cache
def load_from_file(file=CONFIG_FILE_PATH, override_key=OVERRIDE_KEY):
    with open(file) as f:
        data = yaml.safe_load(f)

    config = data.get("defaults", {})
    overrides = data.get("overrides", {}).get(override_key, {})
    for key, value in overrides.items():
        config[key] = value

    return config


def get(*args, **kwargs):
    return load_from_file().get(*args, **kwargs)
