import argparse
import json
import os
import shutil
import subprocess
import time

import yaml
from typing import Any, List, Dict, Optional


def _read_yaml(path: str):
    with open(path, "rt") as f:
        return yaml.safe_load(f)


def _update_docker_compose(docker_compose_path: str, project_name: str,
                           status: Optional[Dict[str, Any]]) -> bool:
    docker_compose_config = _read_yaml(docker_compose_path)

    if not docker_compose_config:
        print("Docker compose currently empty")
        return False

    cmd = ["up", "-d"]
    if status and len(status) > 0:
        cmd += ["--no-recreate"]

    shutdown = False
    if not docker_compose_config["services"]:
        # If no more nodes, run `down` instead of `up`
        print("Shutting down nodes")
        cmd = ["down"]
        shutdown = True
    try:
        subprocess.check_output(
            ["docker-compose", "-f", docker_compose_path, "-p", project_name] +
            cmd + [
                "--remove-orphans",
            ])
    except Exception as e:
        print(f"Ran into error when updating docker compose: {e}")
        # Ignore error

    return shutdown


def _get_ip(project_name: str,
            container_name: str,
            override_network: Optional[str] = None,
            retry_times: int = 3) -> Optional[str]:
    network = override_network or f"{project_name}_ray_local"

    cmd = [
        "docker", "inspect", "-f", "\"{{ .NetworkSettings.Networks"
        f".{network}.IPAddress"
        " }}\"", f"{container_name}"
    ]
    for i in range(retry_times):
        try:
            ip_address = subprocess.check_output(cmd, encoding="utf-8")
        except Exception:
            time.sleep(1)
        else:
            return ip_address.strip().strip("\"").strip("\\\"")
    return None


def _update_docker_status(docker_compose_path: str, project_name: str,
                          docker_status_path: str):
    try:
        data_str = subprocess.check_output([
            "docker-compose",
            "-f",
            docker_compose_path,
            "-p",
            project_name,
            "ps",
            "--format",
            "json",
        ])
        data: List[Dict[str, str]] = json.loads(data_str)
    except Exception as e:
        print(f"Ran into error when fetching status: {e}")
        return None

    status = {}
    for container in data:
        node_id = container["Service"]
        container_name = container["Name"]
        ip = _get_ip(project_name, container_name)
        container["IP"] = ip
        status[node_id] = container

    with open(docker_status_path, "wt") as f:
        json.dump(status, f)

    return status


def monitor_docker(docker_compose_path: str,
                   status_path: str,
                   project_name: str,
                   update_interval: float = 1.):
    while not os.path.exists(docker_compose_path):
        # Wait until cluster is created
        time.sleep(0.5)
        continue

    print("Docker compose config detected, starting status monitoring")

    docker_config = {"force_update": True}

    # Force update
    next_update = time.monotonic() - 1.
    shutdown = False
    status = None
    while not shutdown:
        new_docker_config = _read_yaml(docker_compose_path)
        if new_docker_config != docker_config:
            # Update cluster
            shutdown = _update_docker_compose(docker_compose_path,
                                              project_name, status)

            # Force status update
            next_update = time.monotonic() - 1.

        if time.monotonic() > next_update:
            # Update docker status
            status = _update_docker_status(docker_compose_path, project_name,
                                           status_path)
            next_update = time.monotonic() + update_interval

        docker_config = new_docker_config
        time.sleep(0.1)

    print("Cluster shut down, terminating monitoring script.")


def start_monitor(config_file: str):
    cluster_config = _read_yaml(config_file)

    provider_config = cluster_config["provider"]
    assert provider_config["type"] == "fake_multinode", (
        f"The docker monitor only works with providers of type "
        f"`fake-multinode`, got `{provider_config['type']}`")
    assert provider_config.get("docker", False) is True, (
        "The docker monitor only works with provider configs that set "
        "`docker=True`")

    project_name = provider_config["project_name"]

    volume_dir = provider_config["shared_volume_dir"]
    os.makedirs(volume_dir, mode=0o755, exist_ok=True)

    boostrap_config_path = os.path.join(volume_dir, "bootstrap_config.yaml")
    shutil.copy(config_file, boostrap_config_path)

    docker_compose_config_path = os.path.join(volume_dir,
                                              "docker-compose.yaml")

    # node_state_path = os.path.join(volume_dir, "nodes.json")
    docker_status_path = os.path.join(volume_dir, "status.json")

    if os.path.exists(docker_compose_config_path):
        os.remove(docker_compose_config_path)

    if os.path.exists(docker_status_path):
        os.remove(docker_status_path)
        # Create empty file so it can be mounted
        with open(docker_status_path, "wt") as f:
            f.write("{}")

    print(f"Starting monitor process. Please start Ray cluster with:\n"
          f"   RAY_FAKE_CLUSTER=1 ray up {config_file}")
    monitor_docker(docker_compose_config_path, docker_status_path,
                   project_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", )
    args = parser.parse_args()

    start_monitor(args.config_file)
