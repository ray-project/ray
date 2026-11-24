import re
import shlex
import sys
from pathlib import Path

import pytest
import runfiles
import yaml

from ci.ray_ci.automation import push_ray_image
from ci.ray_ci.automation.trivy import build_report
from ci.ray_ci.automation.trivy.build_report import expected_images
from ci.ray_ci.ray_image import RayImage

PIPELINE = {
    "group": "image scan",
    "steps": [
        {"key": "trivy_db", "commands": ["bazel run //ci/x:trivy_db -- --output m"]},
        {
            "key": "images_scan",
            "commands": [
                "bazel run //ci/ray_ci/automation/trivy:scan_images -- "
                "--architecture aarch64 --platform {{array.platform}} "
                "--image ray:3.12 --image ray-extra:3.12"
            ],
            "array": {"platform": ["cpu", "cu12.8.1-cudnn"]},
        },
        {
            "key": "images_scan_tpu",
            "commands": [
                "bazel run //ci/ray_ci/automation/trivy:scan_images --"
                " --architecture x86_64 --platform tpu --image ray:3.11"
            ],
        },
    ],
}


def real_pipeline() -> dict:
    path = runfiles.Create().Rlocation(build_report.PIPELINE_RUNFILE)
    return yaml.safe_load(Path(path).read_text())


def test_expected_images_reads_plain_and_array_steps():
    names = [image.name for image in expected_images(PIPELINE)]
    assert names == [
        "ray-py3.12-cpu-aarch64",
        "ray-extra-py3.12-cpu-aarch64",
        "ray-py3.12-cu12.8.1-cudnn-aarch64",
        "ray-extra-py3.12-cu12.8.1-cudnn-aarch64",
        "ray-py3.11-tpu",
    ]
    tpu = expected_images(PIPELINE)[-1]
    assert (tpu.image_type, tpu.python_version, tpu.platform, tpu.architecture) == (
        "ray",
        "3.11",
        "tpu",
        "x86_64",
    )


PUSH_TARGET = "//ci/ray_ci/automation:push_ray_image"
PUSH_FILES = ("build.rayci.yml", "linux_aarch64.rayci.yml")


def published_images(pipeline: dict) -> set:
    """(type, python, platform, arch) of every image the push steps publish,
    read with push_ray_image's own option parser."""
    images = set()
    for step in pipeline.get("steps") or []:
        commands = step.get("commands") or step.get("command") or []
        commands = [commands] if isinstance(commands, str) else commands
        array = step.get("array") or {}
        for command in (c for c in commands if PUSH_TARGET in c):
            for python in array.get("python") or [""]:
                for platform in array.get("platform") or [""]:
                    expanded = command.replace("{{array.python}}", python)
                    expanded = expanded.replace("{{array.platform}}", platform)
                    args = shlex.split(expanded.split(PUSH_TARGET, 1)[1])
                    params = push_ray_image.main.make_context(
                        "push_ray_image",
                        args[args.index("--") + 1 :],
                        resilient_parsing=True,
                    ).params
                    for p in params["platform"]:
                        images.add(
                            (
                                params["image_type"],
                                params["python_version"],
                                p,
                                params["architecture"],
                            )
                        )
    return images


def test_the_scan_steps_cover_exactly_what_the_push_steps_publish():
    """A Python version or platform added to a push step must be scanned too:
    the report's coverage check reads only the scan steps."""
    published = set()
    for name in PUSH_FILES:
        path = runfiles.Create().Rlocation(f"io_ray/.buildkite/{name}")
        published |= published_images(yaml.safe_load(Path(path).read_text()))
    scanned = {
        (i.image_type, i.python_version, i.platform, i.architecture)
        for i in expected_images(real_pipeline())
    }
    assert len(published) == 184
    assert scanned == published


def test_the_pipeline_asks_for_every_published_image_once():
    images = expected_images(real_pipeline())
    names = [image.name for image in images]
    assert len(names) == 184
    assert len(set(names)) == len(names)
    for image in images:
        RayImage(
            image.image_type, image.python_version, image.platform, image.architecture
        ).validate()
    assert {
        "ray-py3.11-cpu",
        "ray-py3.14-tpu",
        "ray-llm-extra-py3.12-cu13.0.0-cudnn",
        "ray-extra-py3.14-cu13.3.1-cudnn-aarch64",
    } <= set(names)


SCAN = "bazel run //ci/ray_ci/automation/trivy:scan_images --"


TPU_SCAN = f"{SCAN} --architecture x86_64 --platform tpu --image ray:3.12"


@pytest.mark.parametrize(
    "step",
    [
        pytest.param({"command": TPU_SCAN}, id="command-string"),
        pytest.param({"command": [TPU_SCAN]}, id="command-list"),
        pytest.param({"commands": TPU_SCAN}, id="commands-string"),
        pytest.param({"commands": ["echo start", TPU_SCAN]}, id="commands-list"),
    ],
)
def test_every_way_buildkite_takes_a_command_is_read(step: dict):
    """Buildkite takes `command` or `commands`, each a string or a list, and
    rayci passes them through; a form left unread would drop its images."""
    pipeline = {"steps": [{"key": "s", **step}]}
    assert [i.name for i in expected_images(pipeline)] == ["ray-py3.12-tpu"]


def test_only_the_scan_command_is_parsed():
    """Another command's options must not leak into the scan's."""
    step = {
        "key": "s",
        "commands": ["bazel run //x:setup -- --architecture aarch64", TPU_SCAN],
    }
    assert [i.name for i in expected_images({"steps": [step]})] == ["ray-py3.12-tpu"]


@pytest.mark.parametrize(
    ("command", "error"),
    [
        pytest.param(
            f"{TPU_SCAN} && echo --image ray:3.11",
            "passes scan_images unexpected arguments ['&&', 'echo']",
            id="trailing-command",
        ),
        pytest.param(
            f"{TPU_SCAN} --bogus x --image ray:3.13",
            "No such option: --bogus",
            id="unknown-option",
        ),
        pytest.param(
            f"{SCAN} --architecture x86_64 --platform tpu --image",
            "Option '--image' requires an argument",
            id="option-without-value",
        ),
    ],
)
def test_a_scan_command_with_anything_unexpected_is_an_error(command: str, error: str):
    """Parsing leniently would drop or invent images without a word."""
    with pytest.raises(ValueError, match=re.escape(error)) as info:
        expected_images({"steps": [{"key": "images_scan_x", "commands": [command]}]})
    assert "images_scan_x" in str(info.value)


@pytest.mark.parametrize(
    ("step", "error"),
    [
        pytest.param(
            {
                "commands": [
                    f"{SCAN} --architecture x86_64 --platform cpu "
                    "--image ray:{{array.python}}"
                ],
                "array": {"python": ["3.12"]},
            },
            "leaves {{array.python}}",
            id="other-array-dimension",
        ),
        pytest.param(
            {"commands": [f"{SCAN} --architecture x86_64 --platform cpu"]},
            "no --image",
            id="no-images",
        ),
        pytest.param(
            {"commands": [f"{SCAN} --platform cpu --image ray:3.12"]},
            "no --architecture",
            id="no-architecture",
        ),
        pytest.param(
            {
                "commands": [
                    "bazel run //ci/ray_ci/automation/trivy:scan_images "
                    "--architecture x86_64 --platform cpu --image ray:3.12"
                ]
            },
            "no `--`",
            id="no-separator",
        ),
        pytest.param(
            {"commands": [f"{SCAN} --architecture x86_64 --platform tpu"] * 2},
            "runs scan_images 2 times",
            id="two-scans",
        ),
        pytest.param(
            {
                "commands": [
                    f"{SCAN} --architecture x86_64 --platform tpu --image ray-llm:3.12"
                ]
            },
            "ray-llm",
            id="unpublished-variant",
        ),
    ],
)
def test_a_scan_step_it_cannot_read_is_an_error(step: dict, error: str):
    """Guessing would make the coverage check quietly wrong."""
    with pytest.raises(ValueError, match=re.escape(error)) as info:
        expected_images({"steps": [{"key": "images_scan_x", **step}]})
    assert "images_scan_x" in str(info.value)


def test_two_steps_scanning_the_same_image_are_rejected():
    pipeline = {"steps": [PIPELINE["steps"][2], PIPELINE["steps"][2]]}
    with pytest.raises(ValueError, match="ray-py3.11-tpu"):
        expected_images(pipeline)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
