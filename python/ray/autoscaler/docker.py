from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

logger = logging.getLogger(__name__)


def dockerize_if_needed(config):
    if "docker" not in config:
        return config
    docker_image = config["docker"].get("image")
    cname = config["docker"].get("container_name")
    if not docker_image:
        if cname:
            logger.warning(
                "Container name given but no Docker image - continuing...")
        return config
    else:
        assert cname, "Must provide container name!"
    docker_mounts = {dst: dst for dst in config["file_mounts"]}
    config["setup_commands"] = (
        docker_install_cmds() + docker_start_cmds(
            config["auth"]["ssh_user"], docker_image, docker_mounts, cname) +
        with_docker_exec(config["setup_commands"], container_name=cname))

    config["head_setup_commands"] = with_docker_exec(
        config["head_setup_commands"], container_name=cname)
    config["head_start_ray_commands"] = (
        docker_autoscaler_setup(cname) + with_docker_exec(
            config["head_start_ray_commands"], container_name=cname))

    config["worker_setup_commands"] = with_docker_exec(
        config["worker_setup_commands"], container_name=cname)
    config["worker_start_ray_commands"] = with_docker_exec(
        config["worker_start_ray_commands"],
        container_name=cname,
        env_vars=["RAY_HEAD_IP"])

    return config


def with_docker_exec(cmds, container_name, env_vars=None):
    env_str = ""
    if env_vars:
        env_str = " ".join(
            ["-e {env}=${env}".format(env=env) for env in env_vars])
    return [
        "docker exec {} {} /bin/sh -c {} ".format(env_str, container_name,
                                                  quote(cmd)) for cmd in cmds
    ]


def docker_install_cmds():
    return [
        aptwait_cmd() + " && sudo apt-get update",
        aptwait_cmd() + " && sudo apt-get install -y docker.io"
    ]


def aptwait_cmd():
    return ("while sudo fuser"
            " /var/{lib/{dpkg,apt/lists},cache/apt/archives}/lock"
            " >/dev/null 2>&1; "
            "do echo 'Waiting for release of dpkg/apt locks'; sleep 5; done")


def docker_start_cmds(user, image, mount, cname):
    cmds = []
    cmds.append("sudo kill -SIGUSR1 $(pidof dockerd) || true")
    cmds.append("sudo service docker start")
    cmds.append("sudo usermod -a -G docker {}".format(user))
    cmds.append("docker rm -f {} || true".format(cname))
    cmds.append("docker pull {}".format(image))

    # create flags
    # ports for the redis, object manager, and tune client
    port_flags = " ".join([
        "-p {port}:{port}".format(port=port)
        for port in ["6379", "8076", "4321"]
    ])
    mount_flags = " ".join(
        ["-v {src}:{dest}".format(src=k, dest=v) for k, v in mount.items()])

    # for click, used in ray cli
    env_vars = {"LC_ALL": "C.UTF-8", "LANG": "C.UTF-8"}
    env_flags = " ".join(
        ["-e {name}={val}".format(name=k, val=v) for k, v in env_vars.items()])

    # docker run command
    docker_run = [
        "docker", "run", "--rm", "--name {}".format(cname), "-d", "-it",
        port_flags, mount_flags, env_flags, "--net=host", image, "bash"
    ]
    cmds.append(" ".join(docker_run))
    docker_update = []
    docker_update.append("apt-get -y update")
    docker_update.append("apt-get -y upgrade")
    docker_update.append("apt-get install -y git wget cmake psmisc")
    cmds.extend(with_docker_exec(docker_update, container_name=cname))
    return cmds


def docker_autoscaler_setup(cname):
    cmds = []
    for path in ["~/ray_bootstrap_config.yaml", "~/ray_bootstrap_key.pem"]:
        # needed because docker doesn't allow relative paths
        base_path = os.path.basename(path)
        cmds.append("docker cp {path} {cname}:{dpath}".format(
            path=path, dpath=base_path, cname=cname))
        cmds.extend(
            with_docker_exec(
                ["cp {} {}".format("/" + base_path, path)],
                container_name=cname))
    return cmds
