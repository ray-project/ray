# Adapted from https://github.com/pyg-team/pytorch_geometric/blob/2.5.3
# /examples/multi_gpu/distributed_sampling.py

import argparse
import os

import torch
import torch.nn.functional as F
from filelock import FileLock
from torch_geometric.datasets import FakeDataset, Reddit
from torch_geometric.loader import NeighborLoader
from torch_geometric.nn import SAGEConv
from torch_geometric.transforms import RandomNodeSplit

from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


class SAGE(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels, num_layers=2):
        super().__init__()
        self.num_layers = num_layers

        self.convs = torch.nn.ModuleList()
        self.convs.append(SAGEConv(in_channels, hidden_channels))
        for _ in range(self.num_layers - 2):
            self.convs.append(SAGEConv(hidden_channels, hidden_channels))
        self.convs.append(SAGEConv(hidden_channels, out_channels))

    def forward(self, x, edge_index):
        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            if i != self.num_layers - 1:
                x = F.relu(x)
                x = F.dropout(x, p=0.5, training=self.training)
        return x.log_softmax(dim=-1)

    @torch.no_grad()
    def inference(self, x_all, subgraph_loader, device):
        # Layer-wise inference over the full graph: the representations of all
        # nodes are computed one layer at a time, so only a batch of nodes and
        # their direct neighbors need to be on the device at once.
        for i, conv in enumerate(self.convs):
            xs = []
            for batch in subgraph_loader:
                x = x_all[batch.n_id].to(device)
                x = conv(x, batch.edge_index.to(device))
                # The seed nodes of a batch are always placed first.
                x = x[: batch.batch_size]
                if i != self.num_layers - 1:
                    x = F.relu(x)
                xs.append(x.cpu())

            x_all = torch.cat(xs, dim=0)

        return x_all


def train_loop_per_worker(train_loop_config):
    dataset = train_loop_config["dataset_fn"]()
    batch_size = train_loop_config["batch_size"]
    num_epochs = train_loop_config["num_epochs"]

    world_size = train.get_context().get_world_size()
    rank = train.get_context().get_world_rank()
    device = train.torch.get_device()

    data = dataset[0]
    train_idx = data.train_mask.nonzero(as_tuple=False).view(-1)
    train_idx = train_idx.split(train_idx.size(0) // world_size)[rank]

    # Each worker samples mini-batches around its own shard of the training
    # nodes, so no distributed sampler is needed. The loaders yield
    # ``torch_geometric.data.Data`` objects rather than tensors, so they are
    # not passed through ``train.torch.prepare_data_loader``; batches are moved
    # to the device explicitly below.
    train_loader = NeighborLoader(
        data,
        input_nodes=train_idx,
        num_neighbors=[25, 10],
        batch_size=batch_size,
        shuffle=True,
    )

    # Do validation on rank 0 worker only.
    if rank == 0:
        subgraph_loader = NeighborLoader(
            data, input_nodes=None, num_neighbors=[-1], batch_size=2048, shuffle=False
        )

    model = SAGE(dataset.num_features, 256, dataset.num_classes)
    model = train.torch.prepare_model(model)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    for epoch in range(num_epochs):
        model.train()

        # Each ``batch`` is the sampled subgraph around ``batch.batch_size`` seed
        # nodes, which are always placed first; ``batch.x`` and ``batch.y`` hold
        # the features and labels of every node in the subgraph, and
        # ``batch.n_id`` maps them back to the ids in the full graph. See
        # ``torch_geometric.loader.NeighborLoader`` for more info.
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            out = model(batch.x, batch.edge_index)[: batch.batch_size]
            loss = F.nll_loss(out, batch.y[: batch.batch_size])
            loss.backward()
            optimizer.step()

        if rank == 0:
            print(f"Epoch: {epoch:03d}, Loss: {loss:.4f}")

        train_accuracy = validation_accuracy = test_accuracy = None

        # Do validation on rank 0 worker only.
        if rank == 0:
            model.eval()
            # ``prepare_model`` wraps the model in DistributedDataParallel.
            unwrapped_model = getattr(model, "module", model)
            out = unwrapped_model.inference(data.x, subgraph_loader, device)
            res = out.argmax(dim=-1) == data.y
            train_accuracy = int(res[data.train_mask].sum()) / int(
                data.train_mask.sum()
            )
            validation_accuracy = int(res[data.val_mask].sum()) / int(
                data.val_mask.sum()
            )
            test_accuracy = int(res[data.test_mask].sum()) / int(data.test_mask.sum())

        train.report(
            dict(
                train_accuracy=train_accuracy,
                validation_accuracy=validation_accuracy,
                test_accuracy=test_accuracy,
            )
        )


def gen_fake_dataset():
    """Returns a function to be called on each worker that returns a Fake Dataset."""

    # For fake dataset, since the dataset is randomized, we create it once on the
    # driver, and then send the same dataset to all the training workers.
    # Use 10% of nodes for validation and 10% for testing.
    fake_dataset = FakeDataset(transform=RandomNodeSplit(num_val=0.1, num_test=0.1))

    def gen_dataset():
        return fake_dataset

    return gen_dataset


def gen_reddit_dataset():
    """Returns a function to be called on each worker that returns Reddit Dataset."""

    # For Reddit dataset, we have to download the data on each node, so we create the
    # dataset on each training worker.
    with FileLock(os.path.expanduser("~/.reddit_dataset_lock")):
        dataset = Reddit("./data/Reddit")
    return dataset


def train_gnn(
    num_workers=2, use_gpu=False, epochs=3, global_batch_size=32, dataset="reddit"
):
    per_worker_batch_size = global_batch_size // num_workers

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={
            "num_epochs": epochs,
            "batch_size": per_worker_batch_size,
            "dataset_fn": gen_reddit_dataset
            if dataset == "reddit"
            else gen_fake_dataset(),
        },
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
    )
    result = trainer.fit()
    print(result.metrics)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", help="Whether to use GPU for training."
    )
    parser.add_argument(
        "--epochs", type=int, default=3, help="Number of epochs to train for."
    )
    parser.add_argument(
        "--global-batch-size",
        "-b",
        type=int,
        default=32,
        help="Global batch size to use for training.",
    )
    parser.add_argument(
        "--dataset",
        "-d",
        type=str,
        choices=["reddit", "fake"],
        default="reddit",
        help="The dataset to use. Either 'reddit' or 'fake' Defaults to 'reddit'.",
    )

    args, _ = parser.parse_known_args()

    train_gnn(
        num_workers=args.num_workers,
        use_gpu=args.use_gpu,
        epochs=args.epochs,
        global_batch_size=args.global_batch_size,
        dataset=args.dataset,
    )
