import ray
import ray.train as train
from ray.ml.train.integrations.torch import TorchTrainer
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


def get_dataset(a=5, b=10, size=100):
    items = [i / size for i in range(size)]
    dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in items])
    return dataset


def train_func(config):
    # Get the data.
    dataset_shard = train.get_dataset_shard("dataset")
    print(f"dataset_shard.count() = {dataset_shard.count()}")

    # Create a simple model.
    model = nn.Linear(1, 1)
    model = train.torch.prepare_model(model)
    print("building model and distributing completed")

    loss_fn = nn.MSELoss()

    optimizer = torch.optim.SGD(model.parameters(), lr=1e-2)

    while True:
        train_torch_dataset = dataset_shard.to_torch(
            label_column="y",
            feature_columns=["x"],
            label_column_dtype=torch.float,
            feature_column_dtypes=torch.float,
            batch_size=3,
        )

        device = train.torch.get_device()

        # Switch on `train` mode.
        model.train()

        for X, y in train_torch_dataset:
            print(f"X={X}")
            X = X.to(device)
            y = y.to(device)

            # Compute prediction error.
            pred = model(X)
            loss = loss_fn(pred, y)
            print(f"Loss = {loss}")

            # Backpropagation.
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        result = {"test_key": "test_value"}
        print("reporting ...")
        train.report(**result)
        print("done")


if __name__ == "__main__":
    ray.init(num_cpus=12)

    scaling_config = {"num_workers": 2, "use_gpu": False}

    trainer1 = TorchTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
        datasets={"dataset": get_dataset(size=6)},
    )
    trainable_cls1 = ray.remote(num_cpus=1)(trainer1.as_trainable())
    trainable1 = trainable_cls1.remote()

    trainer2 = TorchTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
        datasets={"dataset": get_dataset(size=7)},
    )
    trainable_cls2 = ray.remote(num_cpus=1)(trainer2.as_trainable())
    trainable2 = trainable_cls2.remote()

    # Run some `train()` calls on trainer1.
    results1 = trainable1.train.remote()
    print(ray.get(results1))
    results1 = trainable1.train.remote()
    print(ray.get(results1))

    # Use trainer2.
    results2 = trainable2.train.remote()
    print(ray.get(results2))

    # Use both once more.
    results1 = trainable1.train.remote()
    print(ray.get(results1))
    results2 = trainable2.train.remote()
    print(ray.get(results2))

    print("ok")
