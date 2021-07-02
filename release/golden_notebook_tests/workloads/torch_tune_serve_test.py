import argparse
import json
import os
import time

import ray
import requests
import torch
import torch.nn as nn
import torchvision.transforms as transforms
from filelock import FileLock
from ray import serve, tune
from ray.util.sgd.torch import TorchTrainer, TrainingOperator
from ray.util.sgd.torch.resnet import ResNet18
from ray.util.sgd.utils import override
from torch.utils.data import DataLoader, Subset
from torchvision.datasets import MNIST


def load_mnist_data(train: bool, download: bool):
    transform = transforms.Compose(
        [transforms.ToTensor(),
         transforms.Normalize((0.1307, ), (0.3081, ))])

    with FileLock(".ray.lock"):
        return MNIST(
            root="~/data", train=train, download=download, transform=transform)


class MnistTrainingOperator(TrainingOperator):
    @override(TrainingOperator)
    def setup(self, config):
        # Create model.
        model = ResNet18(config)
        model.conv1 = nn.Conv2d(
            1, 64, kernel_size=7, stride=1, padding=3, bias=False)

        # Create optimizer.
        optimizer = torch.optim.SGD(
            model.parameters(),
            lr=config.get("lr", 0.1),
            momentum=config.get("momentum", 0.9))

        # Load in training and validation data.
        train_dataset = load_mnist_data(True, True)
        validation_dataset = load_mnist_data(False, False)

        if config["test_mode"]:
            train_dataset = Subset(train_dataset, list(range(64)))
            validation_dataset = Subset(validation_dataset, list(range(64)))

        train_loader = DataLoader(
            train_dataset, batch_size=config["batch_size"], num_workers=2)
        validation_loader = DataLoader(
            validation_dataset, batch_size=config["batch_size"], num_workers=2)

        # Create loss.
        criterion = nn.CrossEntropyLoss()

        # Register all components.
        self.model, self.optimizer, self.criterion = self.register(
            models=model, optimizers=optimizer, criterion=criterion)
        self.register_data(
            train_loader=train_loader, validation_loader=validation_loader)


def train_mnist(test_mode=False, num_workers=1, use_gpu=False):
    TorchTrainable = TorchTrainer.as_trainable(
        training_operator_cls=MnistTrainingOperator,
        num_workers=num_workers,
        use_gpu=use_gpu,
        config={
            "test_mode": test_mode,
            "batch_size": 128
        })

    return tune.run(
        TorchTrainable,
        num_samples=1,
        config={"lr": tune.grid_search([1e-4, 1e-3])},
        stop={"training_iteration": 2},
        verbose=1,
        metric="val_loss",
        mode="min",
        checkpoint_at_end=True)


def get_best_model(best_model_checkpoint_path):
    model_state = torch.load(best_model_checkpoint_path)

    model = ResNet18(None)
    model.conv1 = nn.Conv2d(
        1, 64, kernel_size=7, stride=1, padding=3, bias=False)
    model.load_state_dict(model_state["models"][0])
    model_id = ray.put(model)

    return model_id


@serve.deployment(name="mnist", route_prefix="/mnist")
class MnistDeployment:
    def __init__(self, model_id):
        use_cuda = torch.cuda.is_available()
        self.device = torch.device("cuda" if use_cuda else "cpu")
        model = ray.get(model_id).to(self.device)
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


def setup_serve(model_id):
    serve.start(http_options={
        "location": "EveryNode"
    })  # Start on every node so `predict` can hit localhost.
    MnistDeployment.options(
        num_replicas=2, ray_actor_options={
            "num_gpus": 1
        }).deploy(model_id)


@ray.remote
def predict_and_validate(index, image, label):
    def predict(image):
        response = requests.post(
            "http://localhost:8000/mnist",
            json={"image": image.numpy().tolist()})
        try:
            return response.json()["result"]
        except:  # noqa: E722
            return -1

    prediction = predict(image)
    print("Querying model with example #{}. "
          "Label = {}, Prediction = {}, Correct = {}".format(
              index, label, prediction, label == prediction))
    return prediction


def test_predictions(test_mode=False):
    # Load in data
    dataset = load_mnist_data(False, True)
    num_to_test = 10 if test_mode else 1000
    filtered_dataset = [dataset[i] for i in range(num_to_test)]
    images, labels = zip(*filtered_dataset)

    # Remote function calls are done here for parallelism.
    # As a byproduct `predict` can hit localhost.
    predictions = ray.get([
        predict_and_validate.remote(i, images[i], labels[i])
        for i in range(num_to_test)
    ])

    correct = 0
    for label, prediction in zip(labels, predictions):
        if label == prediction:
            correct += 1

    print("Labels = {}. Predictions = {}. {} out of {} are correct.".format(
        list(labels), predictions, correct, num_to_test))

    return correct / float(num_to_test)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")
    args = parser.parse_args()

    start = time.time()

    ray.client("anyscale://").connect()
    num_workers = 2
    use_gpu = True

    print("Training model.")
    analysis = train_mnist(args.smoke_test, num_workers, use_gpu)

    print("Retrieving best model.")
    best_checkpoint = analysis.best_checkpoint
    model_id = get_best_model(best_checkpoint)

    print("Setting up Serve.")
    setup_serve(model_id)

    print("Testing Prediction Service.")
    accuracy = test_predictions(args.smoke_test)

    taken = time.time() - start
    result = {
        "time_taken": taken,
        "accuracy": accuracy,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/torch_tune_serve_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
