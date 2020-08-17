import os
import logging
try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

logger = logging.getLogger(__name__)


def dockerize_if_needed(config):
    if "docker" not in config:
        return config

    docker_image = config["docker"].get("image")
    docker_pull = config["docker"].get("pull_before_run", True)
    cname = config["docker"].get("container_name")

    head_docker_image = config["docker"].get("head_image", docker_image)

    worker_docker_image = config["docker"].get("worker_image", docker_image)
    worker_run_options = config["docker"].get("worker_run_options", [])

    if not docker_image and not (head_docker_image and worker_docker_image):
        if cname:
            logger.warning(
                "dockerize_if_needed: "
                "Container name given but no Docker image(s) - continuing...")
        return config
    else:
        assert cname, "Must provide container name!"
    ssh_user = config["auth"]["ssh_user"]
    docker_mounts = {dst: dst for dst in config["file_mounts"]}

    if docker_pull:
        docker_pull_cmd = "docker pull {}".format(docker_image)
        config["initialization_commands"].append(docker_pull_cmd)

    head_docker_start = docker_start_cmds(cname, head_docker_image, docker_mounts, config["docker"], "head")

    worker_docker_start = docker_start_cmds(cname, worker_docker_image, docker_mounts, config["docker"], "worker")

    config["head_setup_commands"] = head_docker_start + (with_docker_exec(
        config["head_setup_commands"], container_name=cname))
    config["head_start_ray_commands"] = (
        docker_autoscaler_setup(cname) + with_docker_exec(
            config["head_start_ray_commands"], container_name=cname))

    config["worker_setup_commands"] = worker_docker_start + (with_docker_exec(
        config["worker_setup_commands"], container_name=cname))
    config["worker_start_ray_commands"] = with_docker_exec(
        config["worker_start_ray_commands"],
        container_name=cname,
        env_vars=["RAY_HEAD_IP"])

    return config


def with_docker_exec(cmds,
                     container_name,
                     env_vars=None,
                     with_interactive=False):
    env_str = ""
    if env_vars:
        env_str = " ".join(
            ["-e {env}=${env}".format(env=env) for env in env_vars])
    return [
        "docker exec {interactive} {env} {container} /bin/bash -c {cmd} ".
        format(
            interactive="-it" if with_interactive else "",
            env=env_str,
            container=container_name,
            cmd=quote(cmd)) for cmd in cmds
    ]


def check_docker_running_cmd(cname):
    return " ".join(["docker", "inspect", "-f", "'{{.State.Running}}'", cname])


def docker_start_cmds(cname, image, ray_mounts, docker_config, node_type):
    assert node_type in ["worker", "head"]
    run_options = docker_config.get("run_options", [])
    working_dir = docker_config.get("working_dir")
    user_env = docker_config.get("env", [])
    volume_mounts = docker_config.get("container_volume_mounts", [])
    
    run_options.extend(docker_config.get(f"{node_type}_run_options", []))
    working_dir = docker_config.get(f"{node_type}_working_dir", working_dir)
    user_env.extend(docker_config.get(f"{node_type}_env"))
    volume_mounts.extend(docker_config.get(f"{node_type}_container_volume_mounts", []))


    cmds = []

    # TODO(ilr) Move away from defaulting to /root/
    mount_list = ["-v {src}:{dest}".format(src=k, dest=v.replace("~/", "/root/")) for k, v in ray_mounts.items() ]
    mount_list.extend([f"-v {mnt}" for mnt in volume_mounts])
    mount_flags = " ".join()

    # for click, used in ray cli
    env_vars = {"LC_ALL": "C.UTF-8", "LANG": "C.UTF-8"}
    env_list = ["-e {name}={val}".format(name=k, val=v) for k, v in env_vars.items()]
    env_list.extend([f"-e {var}" for var in user_env])
    env_flags = " ".join(env_list)

    run_options_str = " ".join(run_options)
    # TODO(ilr) Check command type
    # docker run command
    docker_check = check_docker_running_cmd(cname) + " || "
    docker_run = [
        "docker", "run", "--rm", "--name {}".format(cname), "-d", "-it",
        mount_flags, env_flags, run_options_str, f"-w {working_dir}" if working_dir else "", "--net=host", image, "bash"
    ]
    cmds.append(docker_check + " ".join(docker_run))

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
