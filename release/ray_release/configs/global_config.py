import os

import yaml
from typing import List
from typing_extensions import TypedDict


class GlobalConfig(TypedDict):
    byod_ray_ecr: str
    byod_ray_cr_repo: str
    byod_ray_ml_cr_repo: str
    byod_ecr: str
    byod_aws_cr: str
    byod_gcp_cr: str
    state_machine_aws_bucket: str
    state_machine_pr_aws_bucket: str
    state_machine_branch_aws_bucket: str
    aws2gce_credentials: str
    ci_pipeline_premerge: List[str]
    ci_pipeline_postmerge: List[str]


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
        byod_ray_ecr=(
            config_content.get("byod", {}).get("ray_ecr")
            or config_content.get("release_byod", {}).get("ray_ecr")
        ),
        byod_ray_cr_repo=(
            config_content.get("byod", {}).get("ray_cr_repo")
            or config_content.get("release_byod", {}).get("ray_cr_repo")
        ),
        byod_ray_ml_cr_repo=(
            config_content.get("byod", {}).get("ray_ml_cr_repo")
            or config_content.get("release_byod", {}).get("ray_ml_cr_repo")
        ),
        byod_ecr=(
            config_content.get("byod", {}).get("byod_ecr")
            or config_content.get("release_byod", {}).get("byod_ecr")
        ),
        byod_aws_cr=(
            config_content.get("byod", {}).get("aws_cr")
            or config_content.get("release_byod", {}).get("aws_cr")
        ),
        byod_gcp_cr=(
            config_content.get("byod", {}).get("gcp_cr")
            or config_content.get("release_byod", {}).get("gcp_cr")
        ),
        aws2gce_credentials=(
            config_content.get("credentials", {}).get("aws2gce")
            or config_content.get("release_byod", {}).get("aws2gce_credentials")
        ),
        state_machine_aws_bucket=config_content.get("state_machine", {}).get(
            "aws_bucket",
        ),
        state_machine_pr_aws_bucket=config_content.get("state_machine", {})
        .get("pr", {})
        .get(
            "aws_bucket",
        ),
        state_machine_branch_aws_bucket=config_content.get("state_machine", {})
        .get("branch", {})
        .get(
            "aws_bucket",
        ),
        ci_pipeline_premerge=config_content.get("ci_pipeline", {}).get("premerge", []),
        ci_pipeline_postmerge=config_content.get("ci_pipeline", {}).get(
            "postmerge", []
        ),
    )
    # setup GCP workload identity federation
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = f"/workdir/{config['aws2gce_credentials']}"
