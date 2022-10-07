import argparse
import atexit
import json
import os
import time
import subprocess

import ray
from ray.air import session
from ray.air.config import ScalingConfig, RunConfig
from ray.tune.tune_config import TuneConfig
import requests
import torch
import torch.nn as nn
import torchvision.transforms as transforms
from filelock import FileLock
from ray import serve, tune, train
from ray.tune.utils.node import _force_on_current_node
from ray.train.torch import TorchTrainer, TorchCheckpoint
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


def train_epoch(dataloader, model, loss_fn, optimizer):
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
        train_dataset, batch_size=config["batch_size"], num_workers=2
    )
    validation_loader = DataLoader(
        validation_dataset, batch_size=config["batch_size"], num_workers=2
    )

    train_loader = train.torch.prepare_data_loader(train_loader)
    validation_loader = train.torch.prepare_data_loader(validation_loader)

    # Create loss.
    criterion = nn.CrossEntropyLoss()

    for epoch_idx in range(2):
        train_epoch(train_loader, model, criterion, optimizer)
        validation_loss = validate_epoch(validation_loader, model, criterion)

        session.report(
            validation_loss,
            checkpoint=TorchCheckpoint.from_state_dict(model.module.state_dict()),
        )


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
        ),
    )

    return tuner.fit()


def get_remote_model(remote_model_checkpoint_path):
    if ray.util.client.ray.is_connected():
        remote_load = ray.remote(get_model)
        remote_load = _force_on_current_node(remote_load)
        return ray.get(remote_load.remote(remote_model_checkpoint_path))
    else:
        get_best_model_remote = ray.remote(get_model)
        return ray.get(get_best_model_remote.remote(remote_model_checkpoint_path))


def get_model(model_checkpoint_path):
    checkpoint_dict = TorchCheckpoint.from_directory(model_checkpoint_path)
    model_state = checkpoint_dict.to_dict()["model"]

    model = resnet18()
    model.conv1 = nn.Conv2d(1, 64, kernel_size=7, stride=1, padding=3, bias=False)
    model.load_state_dict(model_state)

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
    MnistDeployment.options(
        num_replicas=2, ray_actor_options={"num_gpus": bool(use_gpu)}
    ).deploy(model)


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
    analysis = train_mnist(args.smoke_test, num_workers, use_gpu)._experiment_analysis

    print("Retrieving best model.")
    best_checkpoint_path = analysis.get_best_checkpoint(
        analysis.best_trial, return_path=True
    )
    model = get_remote_model(best_checkpoint_path)

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
