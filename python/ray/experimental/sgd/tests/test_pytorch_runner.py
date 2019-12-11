from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import tempfile
import torch
import torch.nn as nn
import torch.distributed as dist
import unittest

from ray.experimental.sgd.pytorch import PyTorchRunner

if sys.version_info >= (3, 3):
    from unittest.mock import MagicMock
else:
    from mock import MagicMock

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
    return torch.optim.SGD(model.parameters())


def loss_creator(config):
    return nn.MSELoss()


def create_dataloaders(batch_size, config):
    train_dataset = LinearDataset(2, 5)
    validation_dataset = LinearDataset(2, 5, size=400)
    train_loader = torch.utils.data.DataLoader(train_dataset)
    validation_loader = torch.utils.data.DataLoader(validation_dataset)
    return train_loader, validation_loader


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
        result = runner.step()
        self.assertEqual(mock_function.call_count, 0)
        runner.validate()
        self.assertTrue(mock_function.called)
        self.assertEqual(runner.stats()["epochs"], 3)


    def testStep(self):
        mock_function = MagicMock(returns=dict(mean_accuracy=10))
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
        self.assertEqual(result["epochs"], 3)
        self.assertEqual(runner.stats()["epochs"], 3)

    def testGivens(self):
        def three_model_creator(config):
            return nn.Linear(1, 1), nn.Linear(1, 1), nn.Linear(1, 1)

        def three_optimizer_creator(models, config):
            opts = [torch.optim.SGD(model.parameters()) for model in models]
            return opts[0], opts[1], opts[2]

        runner = PyTorchRunner(
            three_model_creator, single_loader, three_optimizer_creator, loss_creator)
        runner.setup()

        self.assertEqual(len(runner.given_models), 3)
        self.assertEqual(len(runner.given_optimizers), 3)

        runner2 = PyTorchRunner(
            model_creator, single_loader, optimizer_creator, loss_creator)
        runner2.setup()

        self.assertNotEqual(runner2.given_models, runner2.models)
        self.assertNotEqual(runner2.given_optimizers, runner2.optimizers)


    def testMultiLoaders(self):
        def three_data_loader(batch_size, config):
            train_dataset = LinearDataset(2, 5)
            validation_dataset = LinearDataset(2, 5, size=400)
            train_loader = torch.utils.data.DataLoader(train_dataset)
            validation_loader = torch.utils.data.DataLoader(validation_dataset)
            return train_loader, validation_loader, validation_loader

        runner = PyTorchRunner(
            model_creator, three_data_loader, optimizer_creator, loss_creator)
        with self.assertRaises(ValueError):
            runner.setup()

        runner2 = PyTorchRunner(
            model_creator, three_data_loader, optimizer_creator, loss_creator)
        with self.assertRaises(ValueError):
            runner.setup()

    def testSingleLoader(self):
        def single_loader(batch_size, config):
            train_dataset = LinearDataset(2, 5)
            train_loader = torch.utils.data.DataLoader(train_dataset)
            return train_loader

        runner = PyTorchRunner(
            model_creator, single_loader, optimizer_creator, loss_creator)
        runner.setup()
        runner.step()
        with pytest.assertRaises(ValueError):
            runner.validate()

    def testMultiModel(self):
        def model_creator(config):
            return nn.Linear(1, 1), nn.Linear(1, 1), nn.Linear(1, 1)

        def optimizer_creator(models, config):
            opts = [torch.optim.SGD(model.parameters()) for model in models]
            return opts[0], opts[1], opts[2]

        runner = PyTorchRunner(
            model_creator, single_loader, optimizer_creator, loss_creator)
        runner.setup()
        with pytest.assertRaises(ValueError):
            runner.step()



