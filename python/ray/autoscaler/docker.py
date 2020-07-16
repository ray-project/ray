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
    cname = config["docker"].get("container_name")
    run_options = config["docker"].get("run_options", [])

    head_docker_image = config["docker"].get("head_image", docker_image)
    head_run_options = config["docker"].get("head_run_options", [])

    worker_docker_image = config["docker"].get("worker_image", docker_image)
    worker_run_options = config["docker"].get("worker_run_options", [])

    image_present = docker_image or (head_docker_image and worker_docker_image)
    if (not cname) and (not image_present):
        return
    else:
        assert cname and image_present, "Must provide a container & image name"

    ssh_user = config["auth"]["ssh_user"]
    docker_mounts = {dst: dst for dst in config["file_mounts"]}

    head_docker_start = docker_start_cmds(ssh_user, head_docker_image,
                                          docker_mounts, cname,
                                          run_options + head_run_options)

    worker_docker_start = docker_start_cmds(ssh_user, worker_docker_image,
                                            docker_mounts, cname,
                                            run_options + worker_run_options)

    config["docker"]["worker_docker_start"] = worker_docker_start
    config["docker"]["head_docker_start"] = head_docker_start

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
    return " ".join([
        "docker", "inspect", "-f", "'{{.State.Running}}'", cname, "||", "true"
    ])


def check_docker_image(cname):
    return " ".join([
        "docker", "inspect", "-f", "'{{.Config.Image}}'", cname, "||", "true"
    ])


def docker_start_cmds(user, image, mount, cname, user_options):

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

    user_options_str = " ".join(user_options)
    docker_run = [
        "docker", "run", "--rm", "--name {}".format(cname), "-d", "-it",
        port_flags, mount_flags, env_flags, user_options_str, "--net=host",
        image, "bash"
    ]
    return " ".join(docker_run)


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
