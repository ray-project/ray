import os

import yaml
from typing import List
from typing_extensions import TypedDict


class GlobalConfig(TypedDict):
    byod_ray_ecr: str
    byod_ray_cr_repo: str
    byod_ray_ml_cr_repo: str
    byod_ray_llm_cr_repo: str
    byod_ecr: str
    byod_ecr_region: str
    byod_aws_cr: str
    byod_gcp_cr: str
    state_machine_pr_aws_bucket: str
    state_machine_branch_aws_bucket: str
    state_machine_disabled: bool
    aws2gce_credentials: str
    ci_pipeline_premerge: List[str]
    ci_pipeline_postmerge: List[str]
    ci_pipeline_buildkite_secret: str
    release_image_step_ray: str
    release_image_step_ray_ml: str
    release_image_step_ray_llm: str


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
        byod_ray_llm_cr_repo=(
            config_content.get("byod", {}).get("ray_llm_cr_repo")
            or config_content.get("release_byod", {}).get("ray_llm_cr_repo")
        ),
        byod_ecr=(
            config_content.get("byod", {}).get("byod_ecr")
            or config_content.get("release_byod", {}).get("byod_ecr")
        ),
        byod_ecr_region=(
            config_content.get("byod", {}).get("byod_ecr_region")
            or config_content.get("release_byod", {}).get("byod_ecr_region")
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
        state_machine_disabled=config_content.get("state_machine", {}).get(
            "disabled", 0
        )
        == 1,
        ci_pipeline_premerge=config_content.get("ci_pipeline", {}).get("premerge", []),
        ci_pipeline_postmerge=config_content.get("ci_pipeline", {}).get(
            "postmerge", []
        ),
        ci_pipeline_buildkite_secret=config_content.get("ci_pipeline", {}).get(
            "buildkite_secret"
        ),
        kuberay_disabled=config_content.get("kuberay", {}).get("disabled", 0) == 1,
        release_image_step_ray=config_content.get("release_image_step", {}).get("ray"),
        release_image_step_ray_ml=config_content.get("release_image_step", {}).get(
            "ray_ml"
        ),
        release_image_step_ray_llm=config_content.get("release_image_step", {}).get(
            "ray_llm"
        ),
    )
    # setup GCP workload identity federation
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = f"/workdir/{config['aws2gce_credentials']}"
