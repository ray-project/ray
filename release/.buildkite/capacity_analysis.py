import yaml
from pathlib import Path
import copy
import jinja2
import os
import boto3

from build_pipeline import SUITES

ray_dir = Path.cwd().parent.parent
"""
{'m5.2xlarge', 'm5.8xlarge', 'm5.large', 'g4dn.xlarge',
'm5.4xlarge', 'm4.large', 'p3.16xlarge', 'i3.8xlarge',
'm5.16xlarge', 'r5.16xlarge', 'g4dn.16xlarge', 'g4dn.metal',
'm4.16xlarge', 'm5.24xlarge', 'g3.16xlarge', 'g3.4xlarge',
'p2.xlarge', 'r5dn.16xlarge', 'm5.xlarge', 'p3.8xlarge',
'i3.4xlarge', 'm4.xlarge', 'c5.9xlarge', 'g3.8xlarge',
'n2-standard-8'}
"""

# def calc_cpu_requirement(instance_type):
#     pass

# def calc_max_capacity(node_config):
#     # Return max virtual CPUs requirement for this config.
#     instance_type
#     max_workers

instances = set()

test_cpu_req = dict()

for suite, tests in SUITES.items():
    for file_path, tests in tests.items():
        path = file_path.replace("~/ray", str(ray_dir))
        suite_path = "/".join(path.split("/")[:-1])
        with open(path, 'r') as file:
            configs = yaml.full_load(file)

        for config in configs:
            compute_paths = config["cluster"]["compute_template"].split("/")
            compute_template_path = Path(suite_path)
            for compute_path in compute_paths:
                compute_template_path = compute_template_path / compute_path
            with open(compute_template_path, 'r') as file:
                content = file.read()
            env = copy.deepcopy(os.environ)
            content = jinja2.Template(content).render(env=env)

            compute_config = yaml.safe_load(content)
            test_name = config["name"]

            max_workers = compute_config.get("max_workers", None)
            head_node_type = compute_config.get("head_node_type", None)
            head_node_req = {
                head_node_type["instance_type"]: 1
            }
            instances.add(head_node_type["instance_type"])

            worker_node_types = compute_config.get("worker_node_types", None)
            worker_node_req = {}
            for worker_node_type in worker_node_types:
                worker_node_instace = worker_node_type["instance_type"]
                worker_node_req[worker_node_instace] = worker_node_type["max_workers"]
                instances.add(worker_node_instace)
            test_cpu_req[test_name] = {
                "head": head_node_req,
                "worker": worker_node_req
            }



# print(test_cpu_req)

# GCP instances.
instances.remove("n2-standard-8")

instance_dict = {}

client = boto3.client('ec2')
result = client.describe_instance_types(InstanceTypes=list(instances))["InstanceTypes"]

for instance in result:
    num_cpus = instance["VCpuInfo"]["DefaultVCpus"]
    instance_dict[instance["InstanceType"]] = num_cpus
print(instance_dict)


per_suite_cpu_req = {}
per_test_cpu_req = {}
for suite, tests in SUITES.items():
    per_suite_cpu_req[suite] = 0
    for file_path, tests in tests.items():
        path = file_path.replace("~/ray", str(ray_dir))
        suite_path = "/".join(path.split("/")[:-1])
        with open(path, 'r') as file:
            configs = yaml.full_load(file)

        for config in configs:
            test_name = config["name"]
            cpu_req = test_cpu_req[test_name]
            per_test_cpu_req[test_name] = 0

            head_req = cpu_req["head"]
            worker_req = cpu_req["worker"]
            for instance, num in head_req.items():
                if instance in instance_dict:
                    per_suite_cpu_req[suite] += instance_dict[instance] * num
                    per_test_cpu_req[test_name] += instance_dict[instance] * num
            for instance, num in worker_req.items():
                if instance in instance_dict:
                    per_suite_cpu_req[suite] += instance_dict[instance] * num
                    per_test_cpu_req[test_name] += instance_dict[instance] * num
print(per_suite_cpu_req)
print(per_test_cpu_req)
