import argparse
import os
import subprocess

import ray
import ray.util.scheduling_strategies


def force_on_node(node_id: str, remote_func_or_actor_class):
    scheduling_strategy = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=node_id, soft=False
    )
    options = {"scheduling_strategy": scheduling_strategy}
    return remote_func_or_actor_class.options(**options)


def run_on_every_node(remote_func_or_actor_class, *remote_args, **remote_kwargs):
    refs = []
    for node in ray.nodes():
        if node["Alive"] and node["Resources"].get("GPU", None):
            refs.append(
                force_on_node(node["NodeID"], remote_func_or_actor_class).remote(
                    *remote_args, **remote_kwargs
                )
            )
    return ray.get(refs)


@ray.remote(num_gpus=1)
def mount_nvme():
    if os.path.exists("/nvme"):
        return
    subprocess.run(
        'drive_name="${1:-/dev/nvme1n1}"; mount_path="${2:-/nvme}"; set -x; sudo file -s "$drive_name"; sudo apt install xfsprogs -y; sudo mkfs -t xfs "$drive_name"; sudo mkdir "$mount_path" && sudo mount "$drive_name" "$mount_path" && sudo chown -R ray "$mount_path"',
        shell=True,
        check=True,
    )


@ray.remote(num_gpus=1)
def download_model(base_model_name=None):
    base_model_name = (
        base_model_name or "RWKV-4-Pile-1B5"
    )  # "RWKV-4-Pile-1B5", "RWKV-4-Pile-430M", "RWKV-4-Pile-169M"
    base_model_url = f"https://huggingface.co/BlinkDL/{base_model_name.lower()}"
    subprocess.run(
        f"cd /nvme; git lfs clone {base_model_url}; ls '{base_model_name.lower()}'",
        shell=True,
        check=True,
    )


@ray.remote(num_gpus=1)
def download_pile_remote(dataset_name):
    subprocess.run(
        "rm -rf /nvme/enwik8; rm -rf /nvme/data/pile/; rm -rf ~/gpt-neox",
        shell=True,
        check=True,
    )
    subprocess.run(
        "cd ~/; git clone https://github.com/Yard1/gpt-neox.git;", shell=True
    )
    subprocess.run(
        f"cd ~/; cd gpt-neox; echo 'starting dataset download {dataset_name}'; python prepare_data.py {dataset_name} -d /nvme/data/pile -t HFTokenizer --vocab-file '/mnt/cluster_storage/20B_tokenizer.json' && echo 'download complete'",
        shell=True,
        check=True,
    )


def download_pile(dataset_name):
    subprocess.run(
        # Necessary for gpt-neox tokenizer to work
        "pip uninstall -y deepspeed && pip install --user -U git+https://github.com/EleutherAI/DeeperSpeed.git@eb7f5cff36678625d23db8a8fe78b4a93e5d2c75#egg=deepspeed",
        shell=True,
    )
    try:
        run_on_every_node(download_pile_remote, dataset_name=dataset_name)
    finally:
        subprocess.run(
            # Use latest deepspeed for actual training. Will crash otherwise
            "pip uninstall -y deepspeed && pip install -U --user deepspeed",
            shell=True,
        )


@ray.remote(num_gpus=1)
def clean_cache():
    subprocess.run("rm -rf  ~/.cache/torch_extensions", shell=True, check=True)


@ray.remote(num_gpus=1)
def run(cmd: str):
    subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("function", type=str, help="function in this file to run")
    parser.add_argument("args", nargs="*", type=str, help="string args to function")
    args = parser.parse_args()

    ray.init()
    if args.function not in globals():
        raise ValueError(f"{args.function} doesn't exist")
    fn = globals()[args.function]
    assert callable(fn) or hasattr(fn, "_function")
    print(f"Running {args.function}({', '.join(args.args)})")
    if hasattr(fn, "_function"):
        run_on_every_node(fn, *args.args)
    else:
        fn(*args.args)
