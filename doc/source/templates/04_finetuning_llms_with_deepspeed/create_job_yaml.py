from argparse import ArgumentParser

import yaml
import os
import pathlib


def _parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        "--size",
        type=str,
        default="7b",
        choices=["7b", "13b", "70b"],
        help="Size of the model to train",
    )
    parser.add_argument(
        "--as-test", action="store_true", help="Whether to run in test mode"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=0,
        help="Number of times to retry the job if it fails",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="./job.yaml",
        help="The path that job yaml should be stored.",
    )
    parser.add_argument("--compute-config", type=str, help="Path to the compute config")
    parser.add_argument(
        "--cluster-env-build-id",
        type=str,
        help="The build-id of the cluster env to use",
    )
    return parser.parse_args()


def main():
    pargs = _parse_args()

    # Resolve compute config
    compute_config_kwargs = {}
    if pargs.compute_config:
        with open(pargs.compute_config, "r") as f:
            compute_config = yaml.safe_load(f)
        compute_config.update(
            {
                "cloud_id": os.environ["ANYSCALE_CLOUD_ID"],
            }
        )
        compute_config_kwargs.update(compute_config=compute_config)

    # Resolve cluster env config
    cluster_env_config_kwargs = {}
    if pargs.cluster_env_build_id:
        cluster_env_config_kwargs.update(build_id=pargs.cluster_env_build_id)

    base_cmd = f"chmod +x ./run_llama_ft.sh && ./run_llama_ft.sh --size={pargs.size}"
    job_config = {
        "name": f"llama-2-{pargs.size}",
        "entrypoint": base_cmd + (" --as-test" if pargs.as_test else ""),
        "max_retries": pargs.max_retries,
        **compute_config_kwargs,
        **cluster_env_config_kwargs,
    }

    pathlib.Path(os.path.dirname(pargs.output_path)).mkdir(parents=True, exist_ok=True)
    with open(pargs.output_path, "w") as f:
        yaml.safe_dump(job_config, f)
    print("Job config written to ", pargs.output_path)
    print("To submit the job, run:")
    print(f"anyscale job submit {pargs.output_path}")


if __name__ == "__main__":
    main()
