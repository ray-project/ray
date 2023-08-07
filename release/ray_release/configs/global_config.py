import yaml
from typing_extensions import TypedDict


class GlobalConfig(TypedDict):
    byod_ray_ecr: str
    byod_ray_cr_repo: str
    byod_ray_ml_cr_repo: str
    byod_ecr: str
    byod_aws_cr: str
    byod_gcp_cr: str
    state_machine_aws_bucket: str


config = None


def init_global_config(config_file: str):
    """
    Initiate the global configuration singleton.
    """
    global config
    if not config:
        _init_global_config(config_file)


def get_global_config():
    """
    Get the global configuration singleton. Need to be invoked after
    init_global_config().
    """
    global config
    return config


def _init_global_config(config_file: str):
    global config
    config_content = yaml.safe_load(open(config_file, "rt"))
    config = GlobalConfig(
        byod_ray_ecr=config_content["byod"]["ray_ecr"],
        byod_ray_cr_repo=config_content["byod"]["ray_cr_repo"],
        byod_ray_ml_cr_repo=config_content["byod"]["ray_ml_cr_repo"],
        byod_ecr=config_content["byod"]["byod_ecr"],
        byod_aws_cr=config_content["byod"].get("aws_cr"),
        byod_gcp_cr=config_content["byod"].get("gcp_cr"),
        state_machine_aws_bucket=config_content["state_machine"]["aws_bucket"],
    )
