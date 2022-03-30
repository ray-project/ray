import csv
import sys
from typing import List, Tuple, Dict

import boto3


def get_aws_instance_information() -> List[Dict[str, Tuple[int, int]]]:
    rows = []
    client = boto3.client("ec2")

    args = {}
    while True:
        result = client.describe_instance_types(**args)

        for instance in result["InstanceTypes"]:
            num_cpus = instance["VCpuInfo"]["DefaultVCpus"]
            num_gpus = sum(
                gpu["Count"] for gpu in instance.get("GpuInfo", {"Gpus": []})["Gpus"]
            )
            rows.append(
                {
                    "instance": instance["InstanceType"],
                    "cpus": num_cpus,
                    "gpus": num_gpus,
                }
            )

        if "NextToken" not in result:
            break

        args["NextToken"] = result["NextToken"]

    return rows


if __name__ == "__main__":
    rows = []

    rows += get_aws_instance_information()

    writer = csv.DictWriter(fieldnames=["instance", "cpus", "gpus"], f=sys.stdout)
    writer.writeheader()
    for row in sorted(rows, key=lambda item: item["instance"]):
        writer.writerow(row)
