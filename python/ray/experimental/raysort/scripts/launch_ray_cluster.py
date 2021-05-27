import datetime
import subprocess

from absl import app
from absl import flags
from absl import logging
import jinja2

from ray.experimental.raysort import constants

FLAGS = flags.FLAGS
flags.DEFINE_string(
    "cluster_config_template",
    "config/raysort-cluster.yaml.template",
    "path to the cluster config file relative to repository root",
)
flags.DEFINE_integer(
    "num_workers",
    10,
    "number of worker nodes",
    short_name="n",
)
flags.DEFINE_string(
    "worker_type",
    "i3.8xlarge",
    "worker instance type",
    short_name="w",
)
flags.DEFINE_string(
    "head_type",
    "i3.xlarge",
    "head instance type",
    short_name="h",
)
flags.DEFINE_integer(
    "worker_ebs_device_count",
    2,
    "number of EBS storage devices per worker",
)
flags.DEFINE_integer(
    "worker_ebs_device_size",
    500,
    "size in GB of each worker EBS storage device",
)
flags.DEFINE_integer(
    "object_store_memory",
    100 * 1024 * 1024 * 1024,
    "memory reserved for object store per worker",
)
flags.DEFINE_bool(
    "preallocate",
    True,
    "if set, allocate all worker nodes at startup",
)


def run(cmd, **kwargs):
    logging.info("$ " + cmd)
    return subprocess.run(cmd, shell=True, **kwargs)


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


def get_num_nvme_devices():
    if FLAGS.worker_type == "i3.8xlarge":
        return 4
    return 0


def get_nvme_device_variables():
    num_nvme_devices = get_num_nvme_devices()
    if num_nvme_devices == 0:
        return {}
    device_mounts = []
    spill_dirs = []
    for d in range(num_nvme_devices):
        device_mounts.append((f"/dev/nvme{d}n1", f"/mnt/nvme{d}"))
        spill_dirs.append(f"/mnt/nvme{d}/tmp/ray/spill")
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
        "HEAD_TYPE": FLAGS.head_type,
        "MIN_WORKERS": FLAGS.num_workers if FLAGS.preallocate else 0,
        "MAX_WORKERS": FLAGS.num_workers,
        "OBJECT_STORE_MEMORY": FLAGS.object_store_memory,
        "PROM_NODE_EXPORTER_PORT": constants.PROM_NODE_EXPORTER_PORT,
        "PROM_RAY_EXPORTER_PORT": constants.PROM_RAY_EXPORTER_PORT,
        "RUN_ID": get_run_id(),
        "WORKER_TYPE": FLAGS.worker_type,
        "WORKER_EBS_DEVICE_SIZE": FLAGS.worker_ebs_device_size,
    }
    variables.update(get_worker_ebs_device_variables())
    variables.update(get_nvme_device_variables())
    conf = template.render(**variables)
    output_path, _ = template_path.rsplit(".", 1)
    with open(output_path, "w") as fout:
        fout.write(conf)
    return output_path


def launch_ray_cluster(cluster_config_file):
    run(f"ray up -y {cluster_config_file}")


def main(argv):
    del argv  # Unused.
    cluster_config_file = write_cluster_config()
    launch_ray_cluster(cluster_config_file)


if __name__ == "__main__":
    app.run(main)
