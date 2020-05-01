import numpy as np
import os
import torch
import torch.nn as nn
import unittest
from unittest.mock import MagicMock, patch

import ray
from ray.util.sgd.torch.training_operator import TrainingOperator
from ray.util.sgd.torch.distributed_torch_runner import (
    LocalDistributedRunner, clear_dummy_actor)
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


class TestLocalDistributedRunner(unittest.TestCase):
    def setUp(self):
        os.environ.pop("CUDA_VISIBLE_DEVICES", None)
        ray.init(num_cpus=10, num_gpus=4)

    def tearDown(self):
        clear_dummy_actor()
        ray.shutdown()

    def _testWithInitialized(self, init_mock):
        mock_runner = MagicMock()
        mock_runner._set_cuda_device = MagicMock()
        preset_devices = os.environ.get("CUDA_VISIBLE_DEVICES")

        LocalDistributedRunner._try_reserve_and_set_cuda(mock_runner)

        self.assertTrue(mock_runner._set_cuda_device.called)
        local_device = mock_runner._set_cuda_device.call_args[0][0]
        env_set_device = os.environ["CUDA_VISIBLE_DEVICES"]
        self.assertEquals(len(env_set_device), 1)

        if preset_devices:
            self.assertIn(env_set_device, preset_devices.split(","))
            self.assertEquals(local_device, "0")
        else:
            self.assertEquals(local_device, env_set_device)

    def testNoVisibleWithInitialized(self):
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = True
            self._testWithInitialized(init_mock)

    def test2VisibleWithInitialized(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "2,3"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = True
            self._testWithInitialized(init_mock)

    def test1VisibleWithInitialized(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = True
            self._testWithInitialized(init_mock)

    def _testNotInitialized(self, init_mock):
        mock_runner = MagicMock()
        mock_runner._set_cuda_device = MagicMock()
        LocalDistributedRunner._try_reserve_and_set_cuda(mock_runner)
        mock_runner._set_cuda_device.assert_called_with("0")
        self.assertEquals(len(os.environ["CUDA_VISIBLE_DEVICES"]), 1)

    def testNoVisibleNotInitialized(self):
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = False
            self._testNotInitialized(init_mock)

    def test2VisibleNotInitialized(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "2,3"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = False
            self._testNotInitialized(init_mock)

    def test1VisibleNotInitialized(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = False
            self._testNotInitialized(init_mock)

    @patch("torch.cuda.set_device")
    def testSetDevice(self, set_mock):
        mock_runner = MagicMock()
        mock_runner._is_set = False
        LocalDistributedRunner._set_cuda_device(mock_runner, "123")
        self.assertEquals(mock_runner.local_device, "123")
        self.assertTrue(set_mock.called)
        set_mock.assert_called_with(123)
