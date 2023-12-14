import argparse
import atexit
import json
import os
import tempfile
import time
import subprocess

import ray
from ray.train import Checkpoint, ScalingConfig, RunConfig
from ray.tune.tune_config import TuneConfig
import requests
import torch
import torch.nn as nn
import torchvision.transforms as transforms
from filelock import FileLock
from ray import serve, tune, train
from ray.train.torch import TorchTrainer
from ray.tune import Tuner
from torch.utils.data import DataLoader, Subset
from torchvision.datasets import MNIST
from torchvision.models import resnet18


def load_mnist_data(train: bool, download: bool):
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
    )

    with FileLock(os.path.expanduser("~/.ray.lock")):
        return MNIST(
            root=os.path.expanduser("~/data"),
            train=train,
            download=download,
            transform=transform,
        )


def train_epoch(epoch, dataloader, model, loss_fn, optimizer):
    if ray.train.get_context().get_world_size() > 1:
        dataloader.sampler.set_epoch(epoch)

    for X, y in dataloader:
        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def validate_epoch(dataloader, model, loss_fn):
    num_batches = len(dataloader)
    model.eval()
    loss = 0
    with torch.no_grad():
        for X, y in dataloader:
            pred = model(X)
            loss += loss_fn(pred, y).item()
    loss /= num_batches
    result = {"val_loss": loss}
    return result


def training_loop(config):
    # Create model.
    model = resnet18()
    model.conv1 = nn.Conv2d(1, 64, kernel_size=7, stride=1, padding=3, bias=False)
    model = train.torch.prepare_model(model)

    # Create optimizer.
    optimizer = torch.optim.SGD(
        model.parameters(),
        lr=config.get("lr", 0.1),
        momentum=config.get("momentum", 0.9),
    )

    # Load in training and validation data.
    train_dataset = load_mnist_data(True, True)
    validation_dataset = load_mnist_data(False, False)

    if config["test_mode"]:
        train_dataset = Subset(train_dataset, list(range(64)))
        validation_dataset = Subset(validation_dataset, list(range(64)))

    train_loader = DataLoader(
        train_dataset, batch_size=config["batch_size"], num_workers=2, shuffle=True
    )
    validation_loader = DataLoader(
        validation_dataset, batch_size=config["batch_size"], num_workers=2
    )

    train_loader = train.torch.prepare_data_loader(train_loader)
    validation_loader = train.torch.prepare_data_loader(validation_loader)

    # Create loss.
    criterion = nn.CrossEntropyLoss()

    for epoch_idx in range(2):
        train_epoch(epoch_idx, train_loader, model, criterion, optimizer)
        validation_loss = validate_epoch(validation_loader, model, criterion)

        with tempfile.TemporaryDirectory() as tmpdir:
            torch.save(model.module.state_dict(), os.path.join(tmpdir, "model.pt"))
            train.report(validation_loss, checkpoint=Checkpoint.from_directory(tmpdir))


def train_mnist(test_mode=False, num_workers=1, use_gpu=False):
    trainer = TorchTrainer(
        training_loop,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
    )
    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "lr": tune.grid_search([1e-4, 1e-3]),
                "test_mode": test_mode,
                "batch_size": 128,
            }
        },
        tune_config=TuneConfig(
            metric="val_loss",
            mode="min",
            num_samples=1,
        ),
        run_config=RunConfig(
            verbose=1,
            storage_path=(
                "/mnt/cluster_storage"
                if os.path.exists("/mnt/cluster_storage")
                else None
            ),
        ),
    )

    return tuner.fit()


def get_model(checkpoint_dir: str):
    model = resnet18()
    model.conv1 = nn.Conv2d(1, 64, kernel_size=7, stride=1, padding=3, bias=False)

    model_state_dict = torch.load(
        os.path.join(checkpoint_dir, "model.pt"), map_location="cpu"
    )
    model.load_state_dict(model_state_dict)

    return model


@serve.deployment(name="mnist", route_prefix="/mnist")
class MnistDeployment:
    def __init__(self, model):
        use_cuda = torch.cuda.is_available()
        self.device = torch.device("cuda" if use_cuda else "cpu")
        model = model.to(self.device)
        self.model = model

    async def __call__(self, request):
        json_input = await request.json()
        prediction = await self.my_batch_handler(json_input["image"])
        return {"result": prediction.cpu().numpy().tolist()}

    @serve.batch(max_batch_size=4, batch_wait_timeout_s=1)
    async def my_batch_handler(self, images):
        print(f"Processing request with batch size {len(images)}.")
        image_tensors = torch.tensor(images)
        image_tensors = image_tensors.to(self.device)
        outputs = self.model(image_tensors)
        predictions = torch.max(outputs.data, 1)[1]
        return predictions


def setup_serve(model, use_gpu: bool = False):
    serve.start(
        http_options={"location": "EveryNode"}
    )  # Start on every node so `predict` can hit localhost.
    serve.run(
        MnistDeployment.options(
            num_replicas=2, ray_actor_options={"num_gpus": bool(use_gpu)}
        ).bind(model)
    )


@ray.remote
def predict_and_validate(index, image, label):
    def predict(image):
        response = requests.post(
            "http://localhost:8000/mnist", json={"image": image.numpy().tolist()}
        )
        try:
            return response.json()["result"]
        except:  # noqa: E722
            return -1

    prediction = predict(image)
    print(
        "Querying model with example #{}. "
        "Label = {}, Prediction = {}, Correct = {}".format(
            index, label, prediction, label == prediction
        )
    )
    return prediction


def test_predictions(test_mode=False):
    # Load in data
    dataset = load_mnist_data(False, True)
    num_to_test = 10 if test_mode else 1000
    filtered_dataset = [dataset[i] for i in range(num_to_test)]
    images, labels = zip(*filtered_dataset)

    # Remote function calls are done here for parallelism.
    # As a byproduct `predict` can hit localhost.
    predictions = ray.get(
        [
            predict_and_validate.remote(i, images[i], labels[i])
            for i in range(num_to_test)
        ]
    )

    correct = 0
    for label, prediction in zip(labels, predictions):
        if label == prediction:
            correct += 1

    print(
        "Labels = {}. Predictions = {}. {} out of {} are correct.".format(
            list(labels), predictions, correct, num_to_test
        )
    )

    return correct / float(num_to_test)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )
    args = parser.parse_args()

    if os.environ.get("IS_SMOKE_TEST"):
        args.smoke_test = True
        proc = subprocess.Popen(["ray", "start", "--head"])
        proc.wait()
        os.environ["RAY_ADDRESS"] = "ray://localhost:10001"

        def stop_ray():
            subprocess.Popen(["ray", "stop", "--force"]).wait()

        atexit.register(stop_ray)

    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "torch_tune_serve_test")
    if addr is not None and addr.startswith("anyscale://"):
        client = ray.init(address=addr, job_name=job_name)
    else:
        client = ray.init()

    num_workers = 2
    use_gpu = not args.smoke_test

    print("Training model.")
    result_grid = train_mnist(args.smoke_test, num_workers, use_gpu)

    print("Retrieving best model.")
    best_result = result_grid.get_best_result()
    best_checkpoint = best_result.get_best_checkpoint(metric="val_loss", mode="min")
    with best_checkpoint.as_directory() as checkpoint_dir:
        model = get_model(checkpoint_dir)

    print("Setting up Serve.")
    setup_serve(model, use_gpu)

    print("Testing Prediction Service.")
    accuracy = test_predictions(args.smoke_test)

    taken = time.time() - start
    result = {
        "time_taken": taken,
        "accuracy": accuracy,
    }
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/torch_tune_serve_test.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
