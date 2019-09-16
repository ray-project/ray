
import os
import torch
import torch.nn as nn
import torch.nn.functional as F
from filelock import FileLock
from torchvision import datasets, transforms
import argparse
import numpy as np

import ray

TEST_SIZE = 1024

def get_gradients(model):
    grads = []
    for p in model.parameters():
        grad = None if p.grad is None else p.grad.data.cpu().numpy()
        grads.append(grad)
    return grads


def set_gradients(gradients, model):
    for g, p in zip(gradients, model.parameters()):
        if g is not None:
            p.grad = torch.from_numpy(g)


def test(model, test_loader):
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(test_loader):
            if batch_idx * len(data) > TEST_SIZE:
                break
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()
    return 100. * correct / total


class ConvNet(nn.Module):
    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)

    def get_weights(self):
        return {k: v.cpu() for k, v in self.state_dict().items()}

    def set_weights(self, weights):
        self.load_state_dict(weights)


def get_data_loader():
    mnist_transforms = transforms.Compose(
        [transforms.ToTensor(),
         transforms.Normalize((0.1307, ), (0.3081, ))])

    # We add FileLock here because multiple workers will want to
    # download data, and this may cause overwrites since
    # DataLoader is not threadsafe.
    with FileLock(os.path.expanduser("~/data.lock")):
        train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "~/data",
                train=True,
                download=True,
                transform=mnist_transforms),
            batch_size=128,
            shuffle=True)
        test_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "~/data",
                train=False,
                transform=mnist_transforms),
            128,
            shuffle=True)

    return train_loader, test_loader


@ray.remote
class ParameterServer(object):
    def __init__(self, learning_rate):
        self.model = ConvNet()
        self.optimizer = torch.optim.SGD(
            self.model.parameters(), lr=learning_rate)

    def apply_gradients(self, *gradients):
        summed_gradients = [
            np.stack(gradient_zip).sum(axis=0)
            for gradient_zip in zip(*gradients)
        ]
        self.optimizer.zero_grad()
        set_gradients(summed_gradients, self.model)
        self.optimizer.step()
        return self.model.get_weights()

    def get_weights(self):
        return self.model.get_weights()

@ray.remote
class DataWorker(object):
    def __init__(self):
        self.model = ConvNet()
        self.data_iterator = iter(get_data_loader()[0])

    def compute_gradient_on_batch(self, data, target):
        self.model.zero_grad()
        output = self.model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        return get_gradients(self.model)

    def compute_gradients(self, weights):
        self.model.set_weights(weights)
        try:
            xs, ys = next(self.data_iterator)
        except StopIteration:
            self.data_iterator = iter(get_data_loader()[0])
            xs, ys = next(self.data_iterator)
        return self.compute_gradient_on_batch(xs, ys)


def run_sync_parameter_server():
    iterations = 200
    num_workers = 2
    # Create a parameter server.
    model = ConvNet()
    ps = ParameterServer.remote(1e-2)
    test_loader = get_data_loader()[1]
    accuracies = []

    # Create workers.
    workers = [DataWorker.remote() for i in range(num_workers)]
    current_weights = ps.get_weights.remote()
    for i in range(iterations):
        # Compute and apply gradients.
        gradients = [
            worker.compute_gradients.remote(current_weights)
            for worker in workers
        ]
        current_weights = ps.apply_gradients.remote(*gradients)

        if i % 10 == 0:
            # Evaluate the current model.
            model.set_weights(ray.get(current_weights))
            accuracy = test(model, test_loader)
            accuracies += [accuracies]
            print("Iter {}: \taccuracy is {:.1f}".format(i, accuracy))

    print("Final accuracy is {:.1f}.".format(accuracy))
    return accuracies

# ray.init(ignore_reinit_error=True)
# accuracies = run_sync_parameter_server()


def run_async_parameter_server():
    iterations = 100
    num_workers = 4
    # Create a parameter server.
    model = ConvNet()
    ps = ParameterServer.remote(1e-2)
    test_loader = get_data_loader()[1]
    accuracies = []


    # Create workers.
    workers = [DataWorker.remote() for i in range(num_workers)]

    current_weights = ps.get_weights.remote()
    gradients = {
        worker.compute_gradients.remote(current_weights): worker
        for worker in workers
    }
    for i in range(iterations * num_workers):
        [ready_gradient], _ = ray.wait(list(gradients))
        worker = gradients.pop(ready_gradient)

        # Compute and apply gradients.
        current_weights = ps.apply_gradients.remote(*[ready_gradient])
        gradients[worker.compute_gradients.remote(current_weights)] = worker

        if i % 10 == 0:
            # Evaluate the current model.
            model.set_weights(ray.get(current_weights))
            accuracy = test(model, test_loader)
            print("Iter {}: \taccuracy is {:.1f}".format(i, accuracy))

    print("Final accuracy is {:.1f}.".format(accuracy))
    return accuracies


ray.init(ignore_reinit_error=True)
accuracies = run_async_parameter_server()
