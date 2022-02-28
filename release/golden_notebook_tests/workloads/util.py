from pathlib import Path
import importlib.util
import ray
import time


def import_and_execute_test_script(relative_path_to_test_script: str):
    """Imports and executes a module from a path relative to Ray repo root."""
    # get the ray folder
    ray_path = next(
        x for x in Path(__file__).resolve().parents if str(x).endswith("/ray")
    )
    notebook_path = ray_path.joinpath(relative_path_to_test_script)
    assert notebook_path.exists()

    spec = importlib.util.spec_from_file_location("notebook_test", notebook_path)
    notebook_test_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(notebook_test_module)


def wait_for_cluster_client(
    num_nodes: int, max_time_s: int, feedback_interval_s: int = 10
):
    assert ray.is_initialized()
    curr_nodes = 0
    start = time.time()
    next_feedback = start
    max_time = start + max_time_s
    while not curr_nodes >= num_nodes:
        now = time.time()

        if now >= max_time:
            raise RuntimeError(
                f"Maximum wait time reached, but only "
                f"{curr_nodes}/{num_nodes} nodes came up. Aborting."
            )

        if now >= next_feedback:
            passed = now - start
            print(
                f"Waiting for more nodes to come up: "
                f"{curr_nodes}/{num_nodes} "
                f"({passed:.0f} seconds passed)"
            )
            next_feedback = now + feedback_interval_s

        time.sleep(5)
        curr_nodes = len(ray.nodes())

    passed = time.time() - start
    print(
        f"Cluster is up: {curr_nodes}/{num_nodes} nodes online after "
        f"{passed:.0f} seconds"
    )
