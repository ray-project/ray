import numpy as np
import torch
import torch.nn as nn
import unittest
from unittest.mock import MagicMock

from ray.experimental.sgd.pytorch.pytorch_runner import PyTorchRunner


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1000):
        x = np.random.random(size).astype(np.float32) * 10
        x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def model_creator(config):
    return nn.Linear(1, 1)


def optimizer_creator(models, config):
    """Returns optimizer."""
    return torch.optim.SGD(models.parameters(), lr=0.1)


def loss_creator(config):
    return nn.MSELoss()


def single_loader(config):
    return LinearDataset(2, 5)


def create_dataloaders(config):
    return LinearDataset(2, 5), LinearDataset(2, 5, size=400)


class TestPyTorchRunner(unittest.TestCase):
    def testValidate(self):
        mock_function = MagicMock(returns=dict(mean_accuracy=10))
        runner = PyTorchRunner(
            model_creator,
            create_dataloaders,
            optimizer_creator,
            loss_creator,
            validation_function=mock_function)
        runner.setup()
        runner.step()
        runner.step()
        runner.step()
        self.assertEqual(mock_function.call_count, 0)
        runner.validate()
        self.assertTrue(mock_function.called)
        self.assertEqual(runner.stats()["epoch"], 3)

    def testStep(self):
        mock_function = MagicMock(return_value=dict(mean_accuracy=10))
        runner = PyTorchRunner(
            model_creator,
            create_dataloaders,
            optimizer_creator,
            loss_creator,
            train_function=mock_function)
        runner.setup()
        runner.step()
        runner.step()
        result = runner.step()
        self.assertEqual(mock_function.call_count, 3)
        self.assertEqual(result["epoch"], 3)
        self.assertEqual(runner.stats()["epoch"], 3)

    def testGivens(self):
        def three_model_creator(config):
            return nn.Linear(1, 1), nn.Linear(1, 1), nn.Linear(1, 1)

        def three_optimizer_creator(models, config):
            opts = [
                torch.optim.SGD(model.parameters(), lr=0.1) for model in models
            ]
            return opts[0], opts[1], opts[2]

        runner = PyTorchRunner(three_model_creator, single_loader,
                               three_optimizer_creator, loss_creator)
        runner.setup()

        self.assertEqual(len(runner.given_models), 3)
        self.assertEqual(len(runner.given_optimizers), 3)

        runner2 = PyTorchRunner(model_creator, single_loader,
                                optimizer_creator, loss_creator)
        runner2.setup()

        self.assertNotEqual(runner2.given_models, runner2.models)
        self.assertNotEqual(runner2.given_optimizers, runner2.optimizers)

    def testMultiLoaders(self):
        def three_data_loader(config):
            return (LinearDataset(2, 5), LinearDataset(2, 5, size=400),
                    LinearDataset(2, 5, size=400))

        runner = PyTorchRunner(model_creator, three_data_loader,
                               optimizer_creator, loss_creator)
        with self.assertRaises(ValueError):
            runner.setup()

        runner2 = PyTorchRunner(model_creator, three_data_loader,
                                optimizer_creator, loss_creator)
        with self.assertRaises(ValueError):
            runner2.setup()

    def testSingleLoader(self):
        runner = PyTorchRunner(model_creator, single_loader, optimizer_creator,
                               loss_creator)
        runner.setup()
        runner.step()
        with self.assertRaises(ValueError):
            runner.validate()

    def testNativeLoss(self):
        runner = PyTorchRunner(
            model_creator,
            single_loader,
            optimizer_creator,
            loss_creator=nn.MSELoss)
        runner.setup()
        runner.step()

    def testMultiModel(self):
        def multi_model_creator(config):
            return nn.Linear(1, 1), nn.Linear(1, 1), nn.Linear(1, 1)

        def multi_optimizer_creator(models, config):
            opts = [
                torch.optim.SGD(model.parameters(), lr=0.1) for model in models
            ]
            return opts[0], opts[1], opts[2]

        runner = PyTorchRunner(multi_model_creator, single_loader,
                               multi_optimizer_creator, loss_creator)
        runner.setup()
        with self.assertRaises(ValueError):
            runner.step()
