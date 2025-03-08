import os

TEMPLATE_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "base_configs", "templates"
    )
)

MODEL_CONFIGS_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "base_configs", "models"
    )
)

_ROOT_DIR_PATH = os.environ.get(
    "ANYSCALE_WORKING_DIR", os.path.dirname(os.path.realpath(__file__))
)
HF_TOKEN_FILE = os.path.join(_ROOT_DIR_PATH, ".HF_TOKEN")

REFERENCE_BASE_MODEL_ID = "mistralai/Mistral-7B-Instruct-v0.1"
DEFAULT_DEPLOYMENT_CONFIGS_FILE = "default_deployment_configs.yaml"


def get_lora_storage_uri():
    artifact_storage = os.environ.get("ANYSCALE_ARTIFACT_STORAGE", "")
    return f"{artifact_storage}/lora_fine_tuning"
