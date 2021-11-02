import datetime
import subprocess
import time

from absl import app
from absl import flags
from absl import logging
import jinja2

FLAGS = flags.FLAGS
flags.DEFINE_string(
    "cluster_config_template",
    "config/raysort-cluster.yaml.template",
    "path to the cluster config file relative to repository root",
)
flags.DEFINE_integer(
    "num_workers",
    64,
    "number of worker nodes",
    short_name="n",
)
flags.DEFINE_string(
    "worker_type",
    "i3.2xlarge",
    "worker instance type",
    short_name="w",
)
flags.DEFINE_string(
    "head_type",
    None,
    "head instance type; if not set will default to worker_type",
    short_name="h",
)
flags.DEFINE_integer(
    "worker_ebs_device_count",
    0,
    "number of EBS storage devices per worker",
)
flags.DEFINE_integer(
    "worker_ebs_device_size",
    500,
    "size in GB of each worker EBS storage device",
)
flags.DEFINE_float(
    "object_spilling_threshold",
    1.0,
    "threshold at which object store starts to spill",
)
flags.DEFINE_integer(
    "object_store_memory",
    # 0,
    24_000_000_000,
    "memory reserved for object store per worker, use default if set to 0",
)
flags.DEFINE_bool(
    "preallocate",
    True,
    "if set, allocate all worker nodes at startup",
)
flags.DEFINE_bool(
    "new",
    False,
    "if set, will tear down the existing cluster first",
)


def run(cmd, **kwargs):
    logging.info("$ " + cmd)
    ret = subprocess.run(cmd, shell=True, **kwargs)
    ret.check_returncode()
    return ret


def get_run_id():
    now = datetime.datetime.now()
    return now.isoformat()


def ord_to_letter(i):
    return chr(ord('a') + i)


def get_worker_ebs_device_variables():
    if FLAGS.worker_ebs_device_count == 0:
        return {}
    device_mounts = [(f"/dev/xvdb{ord_to_letter(d)}", f"/mnt/ebs{d}")
                     for d in range(FLAGS.worker_ebs_device_count)]
    return {
        "WORKER_EBS_DEVICE_MOUNTS": device_mounts,
    }


def get_nvme_device_offset():
    if FLAGS.worker_type.startswith("i3."):
        return 0
    if FLAGS.worker_type.startswith("i3en."):
        return 1
    return 1


def get_nvme_num_devices():
    num_devices = {
        "i3.4xlarge": 2,
        "i3.8xlarge": 4,
        "i3en.2xlarge": 2,
        "i3en.6xlarge": 2,
        "r5d.4xlarge": 2,
    }
    return num_devices.get(FLAGS.worker_type, 1)


def get_nvme_device_variables():
    num_devices = get_nvme_num_devices()
    offset = get_nvme_device_offset()
    if num_devices == 0:
        return {}
    device_mounts = []
    spill_dirs = []
    for d in range(offset, offset + num_devices):
        device_mounts.append((f"/dev/nvme{d}n1", f"/mnt/nvme{d}"))
        spill_dirs.append(f"/mnt/nvme{d}/tmp/ray")
    spill_dirs_str = ",".join(r"\"" + d + r"\"" for d in spill_dirs)
    return {
        "NVME_DEVICE_MOUNTS": device_mounts,
        "SPILL_DIRS_STR": spill_dirs_str,
    }


def write_cluster_config():
    template_path = FLAGS.cluster_config_template
    assert template_path.endswith(".yaml.template"), template_path
    with open(template_path) as fin:
        template = fin.read()
    template = jinja2.Template(template)
    variables = {
        "HEAD_TYPE": FLAGS.head_type or FLAGS.worker_type,
        "MIN_WORKERS": FLAGS.num_workers if FLAGS.preallocate else 0,
        "MAX_WORKERS": FLAGS.num_workers,
        "OBJECT_SPILLING_THRESHOLD": FLAGS.object_spilling_threshold,
        "OBJECT_STORE_MEMORY": FLAGS.object_store_memory,
        "PROM_NODE_EXPORTER_PORT": 8091,
        "PROM_RAY_EXPORTER_PORT": 8090,
        "RUN_ID": get_run_id(),
        "WORKER_TYPE": FLAGS.worker_type,
        "WORKER_EBS_DEVICE_SIZE": FLAGS.worker_ebs_device_size,
    }
    if FLAGS.object_store_memory == 0:
        variables.pop("OBJECT_STORE_MEMORY")
    variables.update(get_worker_ebs_device_variables())
    variables.update(get_nvme_device_variables())
    conf = template.render(**variables)
    output_path, _ = template_path.rsplit(".", 1)
    with open(output_path, "w") as fout:
        fout.write(conf)
    return output_path


def launch_ray_cluster(cluster_config_file):
    if FLAGS.new:
        run(f"ray down -y {cluster_config_file}")
    run(f"ray up -y {cluster_config_file} --no-config-cache")


def wait_until_ready(cluster_config_file):
    while True:
        proc = run(
            f"ray exec {cluster_config_file} 'ray status'",
            capture_output=True)
        out = proc.stdout.decode("ascii")
        print(out)
        if f"{FLAGS.num_workers} ray.worker.default" in out:
            break
        time.sleep(10)


def main(argv):
    del argv  # Unused.
    cluster_config_file = write_cluster_config()
    launch_ray_cluster(cluster_config_file)
    run("./scripts/forward_dashboards.sh")
    wait_until_ready(cluster_config_file)


if __name__ == "__main__":
    app.run(main)
