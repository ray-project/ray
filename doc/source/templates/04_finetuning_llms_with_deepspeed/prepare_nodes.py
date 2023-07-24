# THIS SHOULD BE HIDDEN IN DOCS AND ONLY RAN IN CI
# Download the model from our S3 mirror as it's faster

import ray
import argparse
import ray.util.scheduling_strategies

from utils import download_model, get_mirror_link

download_model_remote = ray.remote(download_model)


def force_on_node(node_id: str, remote_func_or_actor_class):
    scheduling_strategy = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=node_id, soft=False
    )
    options = {"scheduling_strategy": scheduling_strategy}
    return remote_func_or_actor_class.options(**options)


def run_on_every_node(remote_func_or_actor_class, **remote_kwargs):
    refs = []
    for node in ray.nodes():
        if node["Alive"] and node["Resources"].get("GPU", None):
            refs.append(
                force_on_node(node["NodeID"], remote_func_or_actor_class).remote(
                    **remote_kwargs
                )
            )
    return ray.get(refs)


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--hf-model-id",
        type=str,
        default="tiiuae/falcon-7b",
        help=(
            "The Hugging Face model ID to mirror. This will be used to create "
            "the same folder structure as a huggingface model would have."
        ),
    )

    return parser.parse_args()


def download_model_files_on_all_nodes(hf_model_id: str):
    # For every node in the cluster that has a GPU, download the model
    # from the S3 mirror.

    mirror_uri = get_mirror_link(hf_model_id)

    kwargs = {
        "model_id": hf_model_id,
        "bucket_uri": mirror_uri,
        "s3_sync_args": ["--no-sign-request"],
    }

    _ = run_on_every_node(download_model_remote, **kwargs)


if __name__ == "__main__":

    ray.init(
        runtime_env={"env_vars": {"HF_HOME": "/mnt/local_storage/.cache/huggingface"}}
    )

    pargs = _parse_args()
    download_model_files_on_all_nodes(hf_model_id=pargs.hf_model_id)
