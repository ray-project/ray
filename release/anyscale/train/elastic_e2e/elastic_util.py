import ray
import requests
import subprocess
from torch import nn
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


# Define model
class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28 * 28, 512),
            nn.ReLU(),
            nn.Linear(512, 512),
            nn.ReLU(),
            nn.Linear(512, 10),
            nn.ReLU(),
        )

    def forward(self, x):
        x = self.flatten(x)
        logits = self.linear_relu_stack(x)
        return logits


def terminate_current_instance():
    """Use AWS CLI to terminate current instance."""

    token = requests.put(
        "http://169.254.169.254/latest/api/token",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"},
    ).text
    instance_id = requests.get(
        "http://169.254.169.254/latest/meta-data/instance-id",
        headers={"X-aws-ec2-metadata-token": token},
    ).text
    region = requests.get(
        "http://169.254.169.254/latest/meta-data/placement/region",
        headers={"X-aws-ec2-metadata-token": token},
    ).text
    return subprocess.run(
        [
            "aws",
            "ec2",
            "terminate-instances",
            "--instance-ids",
            instance_id,
            "--region",
            region,
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def terminate_node(node_id: str):
    killer_task = ray.remote(terminate_current_instance).options(
        num_cpus=0,
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
    )
    ray.get(killer_task.remote())
