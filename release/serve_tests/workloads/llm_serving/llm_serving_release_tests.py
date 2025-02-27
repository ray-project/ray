from ..anyscale_service_utils import start_service
from typing import Optional
import click
import os
import anyscale

CLOUD = "serve_release_tests_cloud"


def get_current_compute_config_name():
    """Get the name of the current compute config."""
    cluster_id = os.environ["ANYSCALE_CLUSTER_ID"]
    sdk = anyscale.AnyscaleSDK()
    cluster = sdk.get_cluster(cluster_id)
    return anyscale.compute_config.get(
        name="", _id=cluster.result.cluster_compute_id
    ).name


@click.option("--image-uri", type=str, default=None)
def main(
    image_uri: Optional[str],
):
    llm_application = {
        "import_path": "ray.serve.llm.builders.application_builders:build_openai_app",
        "route_prefix": "/",
        "args": {
            "llm_configs": [
                "./model_config/llm_config.yaml",
            ]
        },
    }
    compute_config = get_current_compute_config_name()

    with start_service(
        service_name="replica-scalability",
        image_uri=image_uri,
        compute_config=compute_config,
        applications=[llm_application],
        working_dir="workloads",
        cloud=CLOUD,
    ) as service_name:
        print(f"{service_name=} started")


if __name__ == "__main__":
    main()
