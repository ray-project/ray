# Adapted from https://github.com/pyg-team/pytorch_geometric/blob/master/examples
# /multi_gpu/distributed_sampling.py.

import os
import argparse
from filelock import FileLock
from ray.air import session

import torch
import torch.nn.functional as F

from torch_geometric.datasets import Reddit, FakeDataset
from torch_geometric.loader import NeighborSampler
from torch_geometric.nn import SAGEConv

from ray import train
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
from torch_geometric.transforms import RandomNodeSplit


class SAGE(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels, num_layers=2):
        super().__init__()
        self.num_layers = num_layers

        self.convs = torch.nn.ModuleList()
        self.convs.append(SAGEConv(in_channels, hidden_channels))
        for _ in range(self.num_layers - 2):
            self.convs.append(SAGEConv(hidden_channels, hidden_channels))
        self.convs.append(SAGEConv(hidden_channels, out_channels))

    def forward(self, x, adjs):
        for i, (edge_index, _, size) in enumerate(adjs):
            x_target = x[: size[1]]  # Target nodes are always placed first.
            x = self.convs[i]((x, x_target), edge_index)
            if i != self.num_layers - 1:
                x = F.relu(x)
                x = F.dropout(x, p=0.5, training=self.training)
        return x.log_softmax(dim=-1)

    @torch.no_grad()
    def test(self, x_all, subgraph_loader):
        for i in range(self.num_layers):
            xs = []
            for batch_size, n_id, adj in subgraph_loader:

                edge_index, _, size = adj
                x = x_all[n_id].to(train.torch.get_device())
                x_target = x[: size[1]]
                x = self.convs[i]((x, x_target), edge_index)
                if i != self.num_layers - 1:
                    x = F.relu(x)
                xs.append(x.cpu())

            x_all = torch.cat(xs, dim=0)

        return x_all


def train_loop_per_worker(train_loop_config):
    dataset = train_loop_config["dataset_fn"]()
    batch_size = train_loop_config["batch_size"]
    num_epochs = train_loop_config["num_epochs"]

    data = dataset[0]
    train_idx = data.train_mask.nonzero(as_tuple=False).view(-1)
    train_idx = train_idx.split(train_idx.size(0) // session.get_world_size())[
        session.get_world_rank()
    ]

    train_loader = NeighborSampler(
        data.edge_index,
        node_idx=train_idx,
        sizes=[25, 10],
        batch_size=batch_size,
        shuffle=True,
    )

    # Disable distributed sampler since the train_loader has already been split above.
    train_loader = train.torch.prepare_data_loader(train_loader, add_dist_sampler=False)

    # Do validation on rank 0 worker only.
    if session.get_world_rank() == 0:
        subgraph_loader = NeighborSampler(
            data.edge_index, node_idx=None, sizes=[-1], batch_size=2048, shuffle=False
        )
        subgraph_loader = train.torch.prepare_data_loader(
            subgraph_loader, add_dist_sampler=False
        )

    model = SAGE(dataset.num_features, 256, dataset.num_classes)
    model = train.torch.prepare_model(model)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    x, y = data.x.to(train.torch.get_device()), data.y.to(train.torch.get_device())

    for epoch in range(num_epochs):
        model.train()

        # ``batch_size`` is the number of samples in the current batch.
        # ``n_id`` are the ids of all the nodes used in the computation. This is
        # needed to pull in the necessary features just for the current batch that is
        # being trained on.
        # ``adjs`` is a list of 3 element tuple consisting of ``(edge_index, e_id,
        # size)`` for each sample in the batch, where ``edge_index``represent the
        # edges of the sampled subgraph, ``e_id`` are the ids of the edges in the
        # sample, and ``size`` holds the shape of the subgraph.
        # See ``torch_geometric.loader.neighbor_sampler.NeighborSampler`` for more info.
        for batch_size, n_id, adjs in train_loader:
            optimizer.zero_grad()
            out = model(x[n_id], adjs)
            loss = F.nll_loss(out, y[n_id[:batch_size]])
            loss.backward()
            optimizer.step()

        if session.get_world_rank() == 0:
            print(f"Epoch: {epoch:03d}, Loss: {loss:.4f}")

        train_accuracy = validation_accuracy = test_accuracy = None

        # Do validation on rank 0 worker only.
        if session.get_world_rank() == 0:
            model.eval()
            with torch.no_grad():
                out = model.module.test(x, subgraph_loader)
            res = out.argmax(dim=-1) == data.y
            train_accuracy = int(res[data.train_mask].sum()) / int(
                data.train_mask.sum()
            )
            validation_accuracy = int(res[data.val_mask].sum()) / int(
                data.val_mask.sum()
            )
            test_accuracy = int(res[data.test_mask].sum()) / int(data.test_mask.sum())

        session.report(
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
