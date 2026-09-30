"""Runtime env test for `local://` working dirs that are already in the image.

This test runs on four nodes. `/home/ray/in_image_working_dir` is baked into
the cluster image by `byod_local_working_dir.sh`, so it exists independently on
every node and Ray should use it in place rather than uploading or unpacking it.

Acceptance criteria: Should run through and print "PASSED"
"""

import os
import time
from pathlib import Path

import ray
from ray._private.test_utils import safe_write_to_results_json
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

IN_IMAGE_DIR = "/home/ray/in_image_working_dir"
LOCAL_URI = f"local://{IN_IMAGE_DIR}"
PY_MODULES_DIR = "/home/ray/in_image_py_modules"
PY_MODULES_URI = f"local://{PY_MODULES_DIR}"
EXPECTED_NUM_NODES = 4
EXPECTED_MARKER = "baked-into-the-image"


def update_progress(result):
    result["last_update"] = time.time()
    safe_write_to_results_json(result)


@ray.remote
def inspect_working_dir():
    import in_image_module

    with open("marker") as f:
        marker = f.read().strip()

    return {
        "node_id": ray.get_runtime_context().get_node_id(),
        "cwd": os.getcwd(),
        "marker": marker,
        "value": in_image_module.get_value(),
        "module_file": in_image_module.__file__,
    }


@ray.remote
def import_from_py_modules():
    import in_image_py_module

    return os.getcwd(), in_image_py_module.get_value(), in_image_py_module.__file__


if __name__ == "__main__":
    start_time = time.time()

    ray.init(address="auto", runtime_env={"working_dir": LOCAL_URI})

    node_ids = [node["NodeID"] for node in ray.nodes() if node["Alive"]]
    assert len(node_ids) == EXPECTED_NUM_NODES, node_ids

    results = ray.get(
        [
            inspect_working_dir.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False)
            ).remote()
            for node_id in node_ids
        ]
    )

    for node_id, result in zip(node_ids, results):
        assert result["node_id"] == node_id, result
        assert Path(result["cwd"]).resolve() == Path(IN_IMAGE_DIR).resolve(), result
        assert result["marker"] == EXPECTED_MARKER, result
        assert result["value"] == 42, result
        assert result["module_file"].startswith(IN_IMAGE_DIR), result

    update_progress(
        {
            "phase": "working_dir",
            "num_nodes": len(node_ids),
            "elapsed_time": time.time() - start_time,
        }
    )

    cwd, value, module_file = ray.get(
        import_from_py_modules.options(
            runtime_env={"py_modules": [PY_MODULES_URI]}
        ).remote()
    )
    assert Path(cwd).resolve() == Path(IN_IMAGE_DIR).resolve(), cwd
    assert value == 7, value
    assert module_file.startswith(PY_MODULES_DIR), module_file

    update_progress(
        {
            "phase": "py_modules",
            "elapsed_time": time.time() - start_time,
        }
    )

    missing_dir_failed = False
    try:
        ray.get(
            inspect_working_dir.options(
                runtime_env={"working_dir": "local:///not/in/the/image"}
            ).remote()
        )
    except Exception as e:
        missing_dir_failed = True
        assert "must already exist on every node" in str(e), e
    assert missing_dir_failed, "A missing local:// directory should fail the task."

    update_progress(
        {
            "phase": "missing_dir",
            "elapsed_time": time.time() - start_time,
        }
    )

    print("PASSED")
