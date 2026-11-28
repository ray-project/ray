"""Run PyTorch on Trainium chips from Ray tasks, the way a GPU test runs on GPUs.

Needs Neuron hardware. Each Ray task asks for Neuron cores, and inside it
TorchNeuron (torch-neuronx) runs a matrix product and a short training loop on
the ``neuron`` device. The matrix product is compared with the CPU result.
"""
import argparse

import ray

from neuron_common import write_result


def run_workload(device: str, steps: int) -> dict:
    """Run a matrix product and a short training loop on ``device``.

    Kept free of Neuron calls so that it also runs with ``device="cpu"``.
    """
    import torch

    torch.manual_seed(0)

    # A matrix product on the device must match the CPU result.
    a = torch.randn(64, 128)
    b = torch.randn(128, 32)
    on_device = a.to(device) @ b.to(device)
    device_type = on_device.device.type
    torch.testing.assert_close(on_device.cpu(), a @ b, rtol=1e-4, atol=1e-4)

    # A few optimizer steps: forward, backward and parameter updates on the device.
    model = torch.nn.Sequential(
        torch.nn.Linear(32, 64), torch.nn.ReLU(), torch.nn.Linear(64, 1)
    ).to(device)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.05)
    x = torch.randn(256, 32)
    y = x.sum(dim=1, keepdim=True)
    x, y = x.to(device), y.to(device)

    losses = []
    for _ in range(steps):
        optimizer.zero_grad()
        loss = torch.nn.functional.mse_loss(model(x), y)
        loss.backward()
        optimizer.step()
        losses.append(loss.item())

    assert all(l == l and l != float("inf") for l in losses), losses
    assert losses[-1] < 0.5 * losses[0], f"loss did not go down: {losses}"
    return {
        "device_type": device_type,
        "first_loss": losses[0],
        "last_loss": losses[-1],
    }


@ray.remote
def train_on_neuron(steps: int) -> dict:
    import os
    import platform

    import torch
    import torch_neuronx
    from torch_neuronx.utils import get_platform_target

    assert torch.neuron.is_available(), "torch sees no Neuron device"
    info = {
        "host": platform.node(),
        "torch": torch.__version__,
        "torch_neuronx": torch_neuronx.__version__,
        # "trn2" or "trn3", from the Neuron runtime.
        "platform": get_platform_target(),
        "torch_device_count": torch.neuron.device_count(),
        "ray_cores": ray.get_runtime_context().get_accelerator_ids()["neuron_cores"],
        "visible_cores": os.environ.get("NEURON_RT_VISIBLE_CORES"),
    }
    info.update(run_workload("neuron", steps))
    assert info["device_type"] == "neuron", info
    return info


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--platform",
        required=True,
        choices=["trn2", "trn3"],
        help="The Trainium generation the cluster must have.",
    )
    parser.add_argument("--tasks", type=int, default=4)
    parser.add_argument("--cores-per-task", type=int, default=1)
    parser.add_argument("--steps", type=int, default=20)
    args = parser.parse_args()

    ray.init()

    # Ray detected the Neuron cores (through neuron-ls) on the Trainium node.
    cores = int(ray.cluster_resources().get("neuron_cores", 0))
    needed = args.tasks * args.cores_per_task
    assert cores >= needed, f"cluster has {cores} neuron_cores, need {needed}"

    # The tasks hold their cores at the same time. The first operation on a
    # device compiles its kernels, so allow for that.
    runs = ray.get(
        [
            train_on_neuron.options(
                resources={"neuron_cores": args.cores_per_task}
            ).remote(args.steps)
            for _ in range(args.tasks)
        ],
        timeout=2400,
    )
    write_result({"neuron_cores": cores, "runs": runs})

    for run in runs:
        assert run["platform"] == args.platform, run
        assert len(run["ray_cores"]) == args.cores_per_task, run
        assert run["visible_cores"] == ",".join(run["ray_cores"]), run
    used = [core for run in runs for core in run["ray_cores"]]
    assert len(set(used)) == len(used), f"tasks shared Neuron cores: {used}"
    print(f"{len(runs)} task(s) trained on {args.platform} Neuron cores {used}.")


if __name__ == "__main__":
    main()
