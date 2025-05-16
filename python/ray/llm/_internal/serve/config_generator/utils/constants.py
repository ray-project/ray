import os
from ray.llm._internal.serve.configs.constants import RAYLLM_HOME_DIR

TEMPLATE_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "base_configs", "templates"
    )
)

HF_TOKEN_FILE = os.path.join(RAYLLM_HOME_DIR, ".HF_TOKEN")

DEFAULT_DEPLOYMENT_CONFIGS_FILE = "default_deployment_configs.yaml"


def get_lora_storage_uri():
    lora_path = os.environ.get(
        "RAYLLM_LORA_STORAGE_URI", os.path.join(RAYLLM_HOME_DIR, "lora_ckpts")
    )
    return lora_path
