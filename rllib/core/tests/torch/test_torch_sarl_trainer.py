import pytest

import numpy as np


import ray

from ray.rllib import SampleBatch

from ray.rllib.core.trainer_runner import TrainerRunner
from ray.rllib.utils.test_utils import check

from ray.rllib.core.tests.torch.modules import (
    TorchIndependentModulesTrainer,
    TorchDummyCompositionModuleTrainer,
    TorchSharedEncoderTrainer,
    TorchSharedEncoderAuxLossTrainer,
)


# ==================== Testing Helpers ==================== #


def error_message_fn_1(model, name_value_being_checked):
    msg = (
        f"model {model}, inside of the DummyCompositionRLModule being "
        "optimized by TorchDummyCompositionModuleTrainer should have the "
        f"same {name_value_being_checked} computed on each of their workers "
        "after each update but they DON'T. Something is probably wrong with "
        "the TorchSARLTrainer or torch DDP."
    )
    return msg


def make_dataset():
    size = 1000
    x = np.arange(0, 10, 10 / size, dtype=np.float32)
    a, b = 2, 5
    y = a * x + b
    return x, y


@pytest.mark.parametrize(
    "trainer_class_fn, networks",
    [
        (TorchIndependentModulesTrainer, ["a", "b"]),
        (TorchDummyCompositionModuleTrainer, ["a", "b"]),
        (TorchSharedEncoderTrainer, ["a", "b", "encoder"]),
        (TorchSharedEncoderAuxLossTrainer, ["a", "b", "encoder"]),
    ],
)
def test_1_torch_sarl_trainer_2_gpu(trainer_class_fn, networks):
    ray.init()
    x, y = make_dataset()
    batch_size = 10

    trainer = TrainerRunner(
        trainer_class_fn, {"num_gpus": 2, "module_config": {}}, framework="torch"
    )

    for i in range(2):
        batch = SampleBatch(
            {
                "x": x[i * batch_size : (i + 1) * batch_size],
                "y": y[i * batch_size : (i + 1) * batch_size],
            }
        )
        results_worker_1, results_worker_2 = trainer.update(batch)
        results_worker_1 = results_worker_1["compiled_results"]
        results_worker_2 = results_worker_2["compiled_results"]
        for network in networks:
            assert (
                results_worker_1[f"{network}_norm"]
                == results_worker_2[f"{network}_norm"]
            ), error_message_fn_1(network, "parameter norm")
            assert (
                results_worker_1[f"{network}_grad_norm"]
                == results_worker_2[f"{network}_grad_norm"]
            ), error_message_fn_1(network, "gradient norm")
    del trainer
    ray.shutdown()


@pytest.mark.parametrize(
    "trainer_class_fn, networks",
    [
        (TorchIndependentModulesTrainer, ["a", "b"]),
        (TorchDummyCompositionModuleTrainer, ["a", "b"]),
        (TorchSharedEncoderTrainer, ["a", "b", "encoder"]),
        (TorchSharedEncoderAuxLossTrainer, ["a", "b", "encoder"]),
    ],
)
def test_gradients_params_same_on_all_configurations(trainer_class_fn, networks):
    results = {}
    for num_gpus in [0, 1, 2]:
        ray.init()
        x, y = make_dataset()
        batch_size = 10
        trainer = TrainerRunner(
            trainer_class_fn,
            {"num_gpus": num_gpus, "module_config": {}},
            framework="torch",
        )

        for i in range(3):
            batch = SampleBatch(
                {
                    "x": x[i * batch_size : (i + 1) * batch_size],
                    "y": y[i * batch_size : (i + 1) * batch_size],
                }
            )
            result = trainer.update(batch)
        results[num_gpus] = result
        ray.shutdown()
    for network in networks:
        ground_truth_num_gpus = 0
        ground_truth_network_norm = results[ground_truth_num_gpus][0][
            "compiled_results"
        ][f"{network}_norm"]
        ground_truth_grad_norm = results[ground_truth_num_gpus][0]["compiled_results"][
            f"{network}_grad_norm"
        ]
        for num_gpus in [1, 2]:
            for worker_id in range(num_gpus):
                check(
                    results[num_gpus][worker_id]["compiled_results"][f"{network}_norm"],
                    ground_truth_network_norm,
                )
                check(
                    results[num_gpus][worker_id]["compiled_results"][
                        f"{network}_grad_norm"
                    ],
                    ground_truth_grad_norm,
                )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
