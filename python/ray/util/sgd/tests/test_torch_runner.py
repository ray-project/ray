import numpy as np
import torch
import torch.nn as nn
import unittest
from unittest.mock import MagicMock

from ray.util.sgd.torch.training_operator import TrainingOperator
from ray.util.sgd.torch.torch_runner import TorchRunner


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


class TestTorchRunner(unittest.TestCase):
    def testValidate(self):
        class MockOperator(TrainingOperator):
            def setup(self, config):
                self.train_epoch = MagicMock(returns=dict(mean_accuracy=10))
                self.validate = MagicMock(returns=dict(mean_accuracy=10))

        runner = TorchRunner(
            model_creator,
            create_dataloaders,
            optimizer_creator,
            loss_creator,
            training_operator_cls=MockOperator)
        runner.setup()
        runner.train_epoch()
        runner.train_epoch()
        result = runner.train_epoch()
        self.assertEqual(runner.training_operator.validate.call_count, 0)
        runner.validate()
        self.assertTrue(runner.training_operator.validate.called)
        self.assertEqual(result["epoch"], 3)

    def testtrain_epoch(self):
        class MockOperator(TrainingOperator):
            def setup(self, config):
                self.count = 0

            def train_epoch(self, *args, **kwargs):
                self.count += 1
                return {"count": self.count}

        runner = TorchRunner(
            model_creator,
            create_dataloaders,
            optimizer_creator,
            loss_creator,
            training_operator_cls=MockOperator)
        runner.setup()
        runner.train_epoch(num_steps=1)
        runner.train_epoch(num_steps=1)
        result = runner.train_epoch()
        self.assertEqual(runner.training_operator.count, 3)
        self.assertEqual(result["count"], 3)
        self.assertEqual(result["epoch"], 3)

    def testGivens(self):
        class MockOperator(TrainingOperator):
            def setup(self, config):
                self.train_epoch = MagicMock(returns=dict(mean_accuracy=10))
                self.validate = MagicMock(returns=dict(mean_accuracy=10))

        def three_model_creator(config):
            return nn.Linear(1, 1), nn.Linear(1, 1), nn.Linear(1, 1)

        def three_optimizer_creator(models, config):
            opts = [
                torch.optim.SGD(model.parameters(), lr=0.1) for model in models
            ]
            return opts[0], opts[1], opts[2]

        runner = TorchRunner(
            three_model_creator,
            single_loader,
            three_optimizer_creator,
            loss_creator,
            training_operator_cls=MockOperator)
        runner.setup()

        self.assertEqual(len(runner.given_models), 3)
        self.assertEqual(len(runner.given_optimizers), 3)

        runner2 = TorchRunner(model_creator, single_loader, optimizer_creator,
                              loss_creator)
        runner2.setup()

        self.assertNotEqual(runner2.given_models, runner2.models)
        self.assertNotEqual(runner2.given_optimizers, runner2.optimizers)

    def testMultiLoaders(self):
        def three_data_loader(config):
            return (LinearDataset(2, 5), LinearDataset(2, 5, size=400),
                    LinearDataset(2, 5, size=400))

        runner = TorchRunner(model_creator, three_data_loader,
                             optimizer_creator, loss_creator)
        with self.assertRaises(ValueError):
            runner.setup()

        runner2 = TorchRunner(model_creator, three_data_loader,
                              optimizer_creator, loss_creator)
        with self.assertRaises(ValueError):
            runner2.setup()

    def testSingleLoader(self):
        runner = TorchRunner(model_creator, single_loader, optimizer_creator,
                             loss_creator)
        runner.setup()
        runner.train_epoch()
        with self.assertRaises(ValueError):
            runner.validate()

    def testNativeLoss(self):
        runner = TorchRunner(
            model_creator,
            single_loader,
            optimizer_creator,
            loss_creator=nn.MSELoss)
        runner.setup()
        runner.train_epoch()

    def testMultiModel(self):
        def multi_model_creator(config):
            return nn.Linear(1, 1), nn.Linear(1, 1), nn.Linear(1, 1)

        def multi_optimizer_creator(models, config):
            opts = [
                torch.optim.SGD(model.parameters(), lr=0.1) for model in models
            ]
            return opts[0], opts[1], opts[2]

        runner = TorchRunner(multi_model_creator, single_loader,
                             multi_optimizer_creator, loss_creator)

        with self.assertRaises(ValueError):
            runner.setup()
