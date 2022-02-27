import numpy as np
import os
import torch
import torch.nn as nn
import unittest
from unittest.mock import MagicMock, patch

import ray
from ray.util.sgd.torch.training_operator import TrainingOperator
from ray.util.sgd.torch.distributed_torch_runner import (
    LocalDistributedRunner,
    clear_dummy_actor,
)
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
    def setUp(self):
        self.Operator = TrainingOperator.from_creators(
            model_creator,
            optimizer_creator,
            create_dataloaders,
            loss_creator=loss_creator,
        )

    def testValidate(self):
        class MockOperator(self.Operator):
            def setup(self, config):
                super(MockOperator, self).setup(config)
                self.train_epoch = MagicMock(returns=dict(mean_accuracy=10))
                self.validate = MagicMock(returns=dict(mean_accuracy=10))

        runner = TorchRunner(training_operator_cls=MockOperator)
        runner.setup_operator()
        runner.train_epoch()
        runner.train_epoch()
        result = runner.train_epoch()
        self.assertEqual(runner.training_operator.validate.call_count, 0)
        runner.validate()
        self.assertTrue(runner.training_operator.validate.called)
        self.assertEqual(result["epoch"], 3)

    def testtrain_epoch(self):
        class MockOperator(self.Operator):
            def setup(self, config):
                super(MockOperator, self).setup(config)
                self.count = 0

            def train_epoch(self, *args, **kwargs):
                self.count += 1
                return {"count": self.count}

        runner = TorchRunner(training_operator_cls=MockOperator)
        runner.setup_operator()
        runner.train_epoch(num_steps=1)
        runner.train_epoch(num_steps=1)
        result = runner.train_epoch()
        self.assertEqual(runner.training_operator.count, 3)
        self.assertEqual(result["count"], 3)
        self.assertEqual(result["epoch"], 3)

    def testGivens(self):
        def three_model_creator(config):
            return nn.Linear(1, 1), nn.Linear(1, 1), nn.Linear(1, 1)

        def three_optimizer_creator(models, config):
            opts = [torch.optim.SGD(model.parameters(), lr=0.1) for model in models]
            return opts[0], opts[1], opts[2]

        class MockOperator(TrainingOperator):
            def setup(self, config):
                models = three_model_creator(config)
                optimizers = three_optimizer_creator(models, config)
                loader = single_loader(config)
                loss = loss_creator(config)
                self.models, self.optimizers, self.criterion = self.register(
                    models=models, optimizers=optimizers, criterion=loss
                )
                self.register_data(train_loader=loader, validation_loader=None)
                self.train_epoch = MagicMock(returns=dict(mean_accuracy=10))
                self.validate = MagicMock(returns=dict(mean_accuracy=10))

        runner = TorchRunner(training_operator_cls=MockOperator)
        runner.setup_operator()

        self.assertEqual(len(runner.given_models), 3)
        self.assertEqual(len(runner.given_optimizers), 3)

        runner2 = TorchRunner(training_operator_cls=self.Operator)
        runner2.setup_operator()

        self.assertNotEqual(runner2.given_models, runner2.models)
        self.assertNotEqual(runner2.given_optimizers, runner2.optimizers)

    def testMultiLoaders(self):
        def three_data_loader(config):
            return (
                LinearDataset(2, 5),
                LinearDataset(2, 5, size=400),
                LinearDataset(2, 5, size=400),
            )

        ThreeOperator = TrainingOperator.from_creators(
            model_creator,
            optimizer_creator,
            three_data_loader,
            loss_creator=loss_creator,
        )

        runner = TorchRunner(training_operator_cls=ThreeOperator)
        with self.assertRaises(ValueError):
            runner.setup_operator()

        runner2 = TorchRunner(training_operator_cls=ThreeOperator)
        with self.assertRaises(ValueError):
            runner2.setup_operator()

    def testSingleLoader(self):
        SingleOperator = TrainingOperator.from_creators(
            model_creator, optimizer_creator, single_loader, loss_creator=loss_creator
        )
        runner = TorchRunner(training_operator_cls=SingleOperator)
        runner.setup_operator()
        runner.train_epoch()
        with self.assertRaises(ValueError):
            runner.validate()

    def testNativeLoss(self):
        NativeOperator = TrainingOperator.from_creators(
            model_creator, optimizer_creator, single_loader, loss_creator=nn.MSELoss
        )
        runner = TorchRunner(training_operator_cls=NativeOperator)
        runner.setup_operator()
        runner.train_epoch()


class TestLocalDistributedRunner(unittest.TestCase):
    def setUp(self):
        os.environ.pop("CUDA_VISIBLE_DEVICES", None)
        ray.init(num_cpus=10, num_gpus=4)

    def tearDown(self):
        clear_dummy_actor()
        ray.shutdown()

    def _testReserveCUDAResource(self, init_mock, num_cpus):
        mock_runner = MagicMock()
        mock_runner._set_cuda_device = MagicMock()
        preset_devices = os.environ.get("CUDA_VISIBLE_DEVICES")

        LocalDistributedRunner._try_reserve_and_set_resources(mock_runner, num_cpus, 1)

        self.assertTrue(mock_runner._set_cuda_device.called)
        local_device = mock_runner._set_cuda_device.call_args[0][0]
        env_set_device = os.environ["CUDA_VISIBLE_DEVICES"]
        self.assertEqual(len(env_set_device), 1)

        if preset_devices:
            visible_devices = preset_devices.split(",")
            self.assertIn(env_set_device, visible_devices)
            device_int = int(local_device)
            self.assertLess(device_int, len(visible_devices))
            self.assertEqual(len(os.environ["CUDA_VISIBLE_DEVICES"]), 1)
        else:
            self.assertEqual(local_device, env_set_device)

    def testNoVisibleWithInitialized(self):
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = True
            self._testReserveCUDAResource(init_mock, 0)

    def testNoVisibleWithInitializedAndReserveCPUResource(self):
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = True
            self._testReserveCUDAResource(init_mock, 2)

    def test1VisibleWithInitialized(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = True
            self._testReserveCUDAResource(init_mock, 0)

    def test2VisibleWithInitialized(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "2,3"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = True
            self._testReserveCUDAResource(init_mock, 0)

    def test2VisibleWithInitializedAndReserveCPUResource(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "2,3"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = True
            self._testReserveCUDAResource(init_mock, 2)

    def test1VisibleNotInitialized(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = False
            self._testReserveCUDAResource(init_mock, 0)

    def test1VisibleNotInitializedAndReserveCPUResource(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = False
            self._testReserveCUDAResource(init_mock, 2)

    def test2VisibleNotInitialized(self):
        os.environ["CUDA_VISIBLE_DEVICES"] = "2,3"
        with patch("torch.cuda.is_initialized") as init_mock:
            init_mock.return_value = False
            self._testReserveCUDAResource(init_mock, 0)

    @patch("torch.cuda.set_device")
    def testSetDevice(self, set_mock):
        mock_runner = MagicMock()
        mock_runner._is_set = False
        LocalDistributedRunner._set_cuda_device(mock_runner, "123")
        self.assertEqual(mock_runner.local_cuda_device, "123")
        self.assertTrue(set_mock.called)
        set_mock.assert_called_with(123)

    def testV1ReserveCPUResources(self):
        mock_runner = MagicMock()
        mock_runner._set_cpu_devices = MagicMock()
        # reserve CPU only
        LocalDistributedRunner._try_reserve_and_set_resources(mock_runner, 4, 0)
        remaining = ray.available_resources()["CPU"]
        self.assertEqual(int(remaining), 6)

    def testV2ReserveCPUResources(self):
        mock_runner = MagicMock()
        mock_runner._set_cpu_devices = MagicMock()
        # reserve CPU and GPU
        LocalDistributedRunner._try_reserve_and_set_resources(mock_runner, 4, 1)
        remaining = ray.available_resources()["CPU"]
        self.assertEqual(int(remaining), 6)
