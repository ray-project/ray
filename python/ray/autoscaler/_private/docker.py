from pathlib import Path
from typing import Any, Dict

from ray.autoscaler._private.cli_logger import cli_logger

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote


def _check_docker_file_mounts(file_mounts: Dict[str, str]) -> None:
    """Checks if files are passed as file_mounts. This is a problem for Docker
    based clusters because when a file is bind-mounted in Docker, updates to
    the file on the host do not always propagate to the container. Using
    directories is recommended.
    """
    for remote, local in file_mounts.items():
        if Path(local).is_file():
            cli_logger.warning(
                f"File Mount: ({remote}:{local}) refers to a file.\n To ensure"
                " this mount updates properly, please use a directory."
            )


def validate_docker_config(config: Dict[str, Any]) -> None:
    """Checks whether the Docker configuration is valid."""
    if "docker" not in config:
        return

    _check_docker_file_mounts(config.get("file_mounts", {}))

    docker_image = config["docker"].get("image")
    cname = config["docker"].get("container_name")

    head_docker_image = config["docker"].get("head_image", docker_image)

    worker_docker_image = config["docker"].get("worker_image", docker_image)

    image_present = docker_image or (head_docker_image and worker_docker_image)
    if (not cname) and (not image_present):
        return
    else:
        assert cname and image_present, "Must provide a container & image name"

    return None


def with_docker_exec(
    cmds, container_name, docker_cmd, env_vars=None, with_interactive=False
):
    assert docker_cmd, "Must provide docker command"
    env_str = ""
    if env_vars:
        env_str = " ".join(["-e {env}=${env}".format(env=env) for env in env_vars])
    return [
        "{docker_cmd} exec {interactive} {env} {container} /bin/bash -c {cmd} ".format(
            docker_cmd=docker_cmd,
            interactive="-it" if with_interactive else "",
            env=env_str,
            container=container_name,
            cmd=quote(cmd),
        )
        for cmd in cmds
    ]


def _check_helper(cname, template, docker_cmd):
    return " ".join(
        [docker_cmd, "inspect", "-f", "'{{" + template + "}}'", cname, "||", "true"]
    )


def check_docker_running_cmd(cname, docker_cmd):
    return _check_helper(cname, ".State.Running", docker_cmd)


def check_bind_mounts_cmd(cname, docker_cmd):
    return _check_helper(cname, "json .Mounts", docker_cmd)


def check_docker_image(cname, docker_cmd):
    return _check_helper(cname, ".Config.Image", docker_cmd)


def docker_start_cmds(
    user,
    image,
    mount_dict,
    container_name,
    user_options,
    cluster_name,
    home_directory,
    docker_cmd,
):
    # Imported here due to circular dependency.
    from ray.autoscaler.sdk import get_docker_host_mount_location

    docker_mount_prefix = get_docker_host_mount_location(cluster_name)
    mount = {f"{docker_mount_prefix}/{dst}": dst for dst in mount_dict}

    mount_flags = " ".join(
        [
            "-v {src}:{dest}".format(src=k, dest=v.replace("~/", home_directory + "/"))
            for k, v in mount.items()
        ]
    )

    # for click, used in ray cli
    env_vars = {"LC_ALL": "C.UTF-8", "LANG": "C.UTF-8"}
    env_flags = " ".join(
        ["-e {name}={val}".format(name=k, val=v) for k, v in env_vars.items()]
    )

    user_options_str = " ".join(user_options)
    docker_run = [
        docker_cmd,
        "run",
        "--rm",
        "--name {}".format(container_name),
        "-d",
        "-it",
        mount_flags,
        env_flags,
        user_options_str,
        "--net=host",
        image,
        "bash",
    ]
    return " ".join(docker_run)
