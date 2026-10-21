"""Run the LLM GPU Bazel suites as right-sized jobs on rkn-gpu-cloud."""

import argparse
import os
import shlex
import signal
import subprocess
from pathlib import Path

import yaml

from ci.ray_ci.container import get_docker_image
from ci.ray_ci.linux_container import LinuxContainer
from ci.ray_ci.tester import _get_test_targets
from ci.ray_ci.utils import ci_init

from ray_release.bazel import bazel_runfile


def main():
    import anyscale
    from anyscale.job.models import JobConfig

    from ray_release.aws import maybe_fetch_api_token

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gpus", type=int, choices=(1, 2, 4), default=1)
    parser.add_argument("--custom-plugin", action="store_true")
    args = parser.parse_args()
    os.chdir(os.environ.get("BUILD_WORKSPACE_DIRECTORY", os.getcwd()))
    ci_init()
    maybe_fetch_api_token()

    registry = get_docker_image("llmgpubuild").split("/")[0]
    password = subprocess.check_output(
        ["aws", "ecr", "get-login-password", "--region", "us-west-2"]
    )
    subprocess.run(
        ["docker", "login", "--username", "AWS", "--password-stdin", registry],
        input=password,
        check=True,
    )

    build_name = "custom-vllm-plugin-gpubuild" if args.custom_plugin else "llmgpubuild"
    container = LinuxContainer(build_name, python_version="3.12")
    container.install_ray()
    only_tags = "gpu"
    except_tags = "manual,requires_2GPU,multi_gpu_4,custom_vllm_plugin"
    if args.custom_plugin:
        only_tags, except_tags = "custom_vllm_plugin", "manual"
    elif args.gpus > 1:
        only_tags = "requires_2GPU" if args.gpus == 2 else "multi_gpu_4"
        except_tags = "manual,custom_vllm_plugin"
    test_targets = _get_test_targets(
        container,
        ["//python/ray/llm/...", "//doc/..."],
        "llm",
        "linux",
        only_tags=only_tags,
        except_tags=except_tags,
        get_high_impact_tests=os.environ.get("RAYCI_MICROCHECK_RUN") == "1",
        lookup_test_database=False,
    )
    if not test_targets:
        print("No LLM GPU tests selected; skipping the H100 job.")
        return

    image_uri = get_docker_image(f"{build_name}-h100-{os.environ['BUILDKITE_JOB_ID']}")
    subprocess.run(
        [
            "docker",
            "build",
            "--build-arg",
            f"BASE_IMAGE={get_docker_image(build_name)}",
            "-f",
            "ci/ray_ci/llm_h100.env.Dockerfile",
            "-t",
            image_uri,
            ".",
        ],
        check=True,
    )
    subprocess.run(["docker", "push", image_uri], check=True)

    command = (
        "cd /rayci && "
        "bazel test --jobs=1 --config=ci $(./ci/run/bazel_export_options) "
        "--test_env=RAY_ADDRESS=local --test_env=VLLM_DISABLE_COMPILE_CACHE=1 "
        "--nocache_test_results " + shlex.join(test_targets)
    )
    compute_file = Path(
        bazel_runfile(f"release/llm_tests/cluster_compute/{args.gpus}xh100.yaml")
    )
    config = {
        "name": f"llm-ci-{os.environ['BUILDKITE_JOB_ID']}",
        "image_uri": image_uri,
        "cloud": "rkn-gpu-cloud",
        "project": "prj_69fd51u496ygpnaza62xsmis64",
        "compute_config": yaml.safe_load(compute_file.read_text()),
        "entrypoint": "bash -iecuo pipefail -- " + shlex.quote(command),
        "max_retries": 0,
        "timeout_s": 7200,
    }
    job_id = anyscale.job.submit(JobConfig(**config))

    def cancel(signum, _frame):
        raise SystemExit(128 + signum)

    previous_handler = signal.signal(signal.SIGTERM, cancel)
    try:
        anyscale.job.wait(id=job_id, timeout_s=7500, follow=True)
    except BaseException:
        anyscale.job.terminate(id=job_id)
        raise
    finally:
        signal.signal(signal.SIGTERM, previous_handler)


if __name__ == "__main__":
    main()
