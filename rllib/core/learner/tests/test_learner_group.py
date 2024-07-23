import gymnasium as gym
import itertools
import numpy as np
import tempfile
import unittest
import pytest

import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo.tests.test_ppo_learner import FAKE_BATCH
from ray.rllib.core import (
    COMPONENT_LEARNER,
    COMPONENT_RL_MODULE,
    DEFAULT_MODULE_ID,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.utils import (
    add_module_to_learner_or_learner_group,
)
from ray.rllib.core.testing.testing_learner import BaseTestingAlgorithmConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader
from ray.rllib.utils.metrics import ALL_MODULES
from ray.util.timer import _Timer


REMOTE_CONFIGS = {
    "remote-cpu": AlgorithmConfig.overrides(num_learners=1),
    "remote-gpu": AlgorithmConfig.overrides(num_learners=1, num_gpus_per_learner=1),
    "multi-gpu-ddp": AlgorithmConfig.overrides(num_learners=2, num_gpus_per_learner=1),
    "multi-cpu-ddp": AlgorithmConfig.overrides(num_learners=2, num_cpus_per_learner=2),
    # "multi-gpu-ddp-pipeline": AlgorithmConfig.overrides(
    #     num_learners=2, num_gpus_per_learner=2
    # ),
}


LOCAL_CONFIGS = {
    "local-cpu": AlgorithmConfig.overrides(num_learners=0, num_gpus_per_learner=0),
    "local-gpu": AlgorithmConfig.overrides(num_learners=0, num_gpus_per_learner=1),
}


# TODO(avnishn) Make this a ray task later. Currently thats not possible because the
#  task is not dying after the test is done. This is a bug with ray core.
@ray.remote(num_gpus=1)
class RemoteTrainingHelper:
    def local_training_helper(self, fw, scaling_mode) -> None:
        if fw == "torch":
            import torch

            torch.manual_seed(0)
        elif fw == "tf2":
            import tensorflow as tf

            # this is done by rllib already inside of the policy class, but we need to
            # do it here for testing purposes
            tf.compat.v1.enable_eager_execution()
            tf.random.set_seed(0)

        env = gym.make("CartPole-v1")

        reader = get_cartpole_dataset_reader(batch_size=500)
        batch = reader.next().as_multi_agent()

        config_overrides = LOCAL_CONFIGS[scaling_mode]
        config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)

        learner_group = config.build_learner_group(env=env)
        local_learner = config.build_learner(env=env)

        # Make the state of the learner and the local learner_group identical.
        local_learner.set_state(learner_group.get_state()[COMPONENT_LEARNER])
        check(local_learner.get_state(), learner_group.get_state()[COMPONENT_LEARNER])

        # Update and check state again.
        learner_update = local_learner.update_from_batch(batch=batch)
        learner_update = tree.map_structure(lambda s: s.peek(), learner_update)
        learner_group_update = learner_group.update_from_batch(batch=batch)
        check(learner_update, learner_group_update)
        check(local_learner.get_state(), learner_group.get_state()[COMPONENT_LEARNER])

        new_module_id = "test_module"

        add_module_to_learner_or_learner_group(
            config, env, new_module_id, learner_group
        )
        add_module_to_learner_or_learner_group(
            config, env, new_module_id, local_learner
        )

        # make the state of the learner and the local learner_group identical
        local_learner.set_state(learner_group.get_state()[COMPONENT_LEARNER])
        check(local_learner.get_state(), learner_group.get_state()[COMPONENT_LEARNER])

        # Do another update.
        batch = reader.next()
        ma_batch = MultiAgentBatch(
            {new_module_id: batch, DEFAULT_MODULE_ID: batch}, env_steps=batch.count
        )
        # the optimizer state is not initialized fully until the first time that
        # training is completed. A call to get state before that won't contain the
        # optimizer state. So we do a dummy update here to initialize the optimizer
        l0 = local_learner.get_state()
        local_learner.update_from_batch(batch=ma_batch)
        l1 = local_learner.get_state()
        check(
            l0["rl_module"]["default_policy"]["policy.0.bias"],
            l1["rl_module"]["default_policy"]["policy.0.bias"],
            false=True,
        )
        check(
            l0["rl_module"]["test_module"]["policy.0.bias"],
            l1["rl_module"]["test_module"]["policy.0.bias"],
            false=True,
        )
        check(
            l0["optimizer"]["default_policy_default_optimizer"]["state"][0]["exp_avg"],
            l1["optimizer"]["default_policy_default_optimizer"]["state"][0]["exp_avg"],
            false=True,
        )
        check(
            l0["optimizer"]["test_module_default_optimizer"]["state"],
            {},
        )

        lg0 = learner_group.get_state()[COMPONENT_LEARNER]
        check(l0, lg0)

        learner_group.update_from_batch(batch=ma_batch)
        lg1 = learner_group.get_state()[COMPONENT_LEARNER]

        check(
            lg0["rl_module"]["default_policy"]["policy.0.bias"],
            lg1["rl_module"]["default_policy"]["policy.0.bias"],
            false=True,
        )
        check(
            lg0["rl_module"]["test_module"]["policy.0.bias"],
            lg1["rl_module"]["test_module"]["policy.0.bias"],
            false=True,
        )
        check(
            lg0["optimizer"]["default_policy_default_optimizer"]["state"][0]["exp_avg"],
            lg1["optimizer"]["default_policy_default_optimizer"]["state"][0]["exp_avg"],
            false=True,
        )
        check(
            lg0["optimizer"]["test_module_default_optimizer"]["state"],
            {},
        )

        check(l1["rl_module"]["test_module"], lg1["rl_module"]["test_module"])
        check(
            l1["optimizer"]["test_module_default_optimizer"],
            lg1["optimizer"]["test_module_default_optimizer"],
        )
        # check(l1["rl_module"]["default_policy"], lg1["rl_module"]["default_policy"])

        # local_learner.update_from_batch(batch=ma_batch)
        # learner_group.update_from_batch(batch=ma_batch)

        # check(local_learner.get_state(), learner_group.get_state()[COMPONENT_LEARNER])
        # local_learner_results = local_learner.update_from_batch(batch=ma_batch)
        # local_learner_results = tree.map_structure(
        #    lambda s: s.peek(), local_learner_results
        # )
        # learner_group_results = learner_group.update_from_batch(batch=ma_batch)

        # check(local_learner_results, learner_group_results)

        # check(local_learner.get_state(), learner_group.get_state()[COMPONENT_LEARNER])


class TestLearnerGroupSyncUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_learner_group_build_from_algorithm_config(self):
        """Tests whether we can build a learner_groupobject from algorithm_config."""

        env = gym.make("CartPole-v1")

        # Config that has its own learner class and RLModule spec.
        config = BaseTestingAlgorithmConfig()
        learner_group = config.build_learner_group(env=env)
        print(learner_group)
        learner_group.shutdown()

        # Config for which user defines custom learner class and RLModule spec.
        config = (
            BaseTestingAlgorithmConfig()
            .training(learner_class=BCTorchLearner)
            .rl_module(
                rl_module_spec=SingleAgentRLModuleSpec(
                    module_class=DiscreteBCTorchModule,
                    model_config_dict={"fcnet_hiddens": [32]},
                )
            )
        )
        learner_group = config.build_learner_group(env=env)
        print(learner_group)
        learner_group.shutdown()

    # def test_learner_group_local(self):
    #    fws = ["torch", "tf2"]

    #    test_iterator = itertools.product(fws, LOCAL_CONFIGS)

    #    # run the logic of this test inside of a ray actor because we want tensorflow
    #    # resources to be gracefully released. Tensorflow blocks the gpu resources
    #    # otherwise between test cases, causing a gpu oom error.
    #    for fw, scaling_mode in test_iterator:
    #        print(f"Testing framework: {fw}, scaling_mode: {scaling_mode}")
    #        training_helper = RemoteTrainingHelper.remote()
    #        ray.get(training_helper.local_training_helper.remote(fw, scaling_mode))
    #        del training_helper

    def test_update_multi_gpu(self):
        return

        fws = ["torch", "tf2"]
        scaling_modes = ["multi-gpu-ddp", "remote-gpu"]
        test_iterator = itertools.product(fws, scaling_modes)

        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")

            config_overrides = REMOTE_CONFIGS[scaling_mode]
            config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)
            learner_group = config.build_learner_group(env=env)
            reader = get_cartpole_dataset_reader(batch_size=1024)

            min_loss = float("inf")
            for iter_i in range(1000):
                batch = reader.next()
                results = learner_group.update_from_batch(batch=batch.as_multi_agent())

                loss = np.mean(
                    [res[ALL_MODULES][Learner.TOTAL_LOSS_KEY] for res in results]
                )
                min_loss = min(loss, min_loss)
                print(f"[iter = {iter_i}] Loss: {loss:.3f}, Min Loss: {min_loss:.3f}")
                # The loss is initially around 0.69 (ln2). When it gets to around
                # 0.57 the return of the policy gets to around 100.
                if min_loss < 0.57:
                    break

                for res1, res2 in zip(results, results[1:]):
                    self.assertEqual(
                        res1[DEFAULT_MODULE_ID]["mean_weight"],
                        res2[DEFAULT_MODULE_ID]["mean_weight"],
                    )

            self.assertLess(min_loss, 0.57)

            # Make sure the learner_group resources are freed up so that we don't
            # autoscale.
            learner_group.shutdown()
            del learner_group

    def test_add_module_and_remove_module(self):
        fws = ["torch", "tf2"]
        scaling_modes = ["local-cpu", "multi-gpu-ddp"]
        test_iterator = itertools.product(fws, scaling_modes)

        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")
            config_overrides = REMOTE_CONFIGS.get(scaling_mode) or LOCAL_CONFIGS.get(
                scaling_mode
            )
            config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)
            learner_group = config.build_learner_group(env=env)
            reader = get_cartpole_dataset_reader(batch_size=512)
            batch = reader.next()

            # Update once with the default policy.
            learner_group.update_from_batch(batch.as_multi_agent())
            module_ids_before_add = {DEFAULT_MODULE_ID}
            new_module_id = "test_module"

            # Add a test_module.
            add_module_to_learner_or_learner_group(
                config, env, new_module_id, learner_group
            )

            # Do training that includes the test_module.
            results = learner_group.update_from_batch(
                batch=MultiAgentBatch(
                    {new_module_id: batch, DEFAULT_MODULE_ID: batch}, batch.count
                ),
            )

            _check_multi_worker_weights(learner_group, results)

            # check that module ids are updated to include the new module
            module_ids_after_add = {DEFAULT_MODULE_ID, new_module_id}
            # remove the total_loss key since its not a module key
            self.assertEqual(set(results.keys()) - {ALL_MODULES}, module_ids_after_add)

            # Remove the test_module.
            learner_group.remove_module(module_id=new_module_id)

            # Run training without the test_module.
            results = learner_group.update_from_batch(batch.as_multi_agent())

            _check_multi_worker_weights(learner_group, results)

            # check that module ids are updated after remove operation to not
            # include the new module
            # remove the total_loss key since its not a module key
            self.assertEqual(set(results.keys()) - {ALL_MODULES}, module_ids_before_add)

            # make sure the learner_group resources are freed up so that we don't
            # autoscale
            learner_group.shutdown()
            del learner_group


class TestLearnerGroupCheckpointRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_restore_from_path_marl_module_and_individual_modules(self):
        """Tests whether MARLModule- and single RLModule states can be restored."""
        fws = ["torch", "tf2"]
        # this is expanded to more scaling modes on the release ci.
        scaling_modes = ["local-cpu", "multi-gpu-ddp"]

        test_iterator = itertools.product(fws, scaling_modes)
        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            # env will have agent ids 0 and 1
            env = MultiAgentCartPole({"num_agents": 2})

            config_overrides = REMOTE_CONFIGS.get(scaling_mode) or LOCAL_CONFIGS.get(
                scaling_mode
            )
            config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)
            learner_group = config.build_learner_group(env=env)
            spec = config.get_marl_module_spec(env=env).module_specs[DEFAULT_MODULE_ID]
            learner_group.add_module(module_id="0", module_spec=spec)
            learner_group.add_module(module_id="1", module_spec=spec)
            learner_group.remove_module(DEFAULT_MODULE_ID)

            module_0 = spec.build()
            module_1 = spec.build()
            marl_module = MultiAgentRLModule()
            marl_module.add_module(module_id="0", module=module_0)
            marl_module.add_module(module_id="1", module=module_1)

            # Check if we can load just the MARL Module.
            with tempfile.TemporaryDirectory() as tmpdir:
                marl_module.save_to_path(tmpdir)
                old_learner_weights = learner_group.get_weights()
                learner_group.restore_from_path(
                    tmpdir,
                    component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE,
                )
                # Check the weights of the module in the learner group are the
                # same as the weights of the newly created marl module
                check(learner_group.get_weights(), marl_module.get_state())
                learner_group.set_state(
                    {
                        COMPONENT_LEARNER: {COMPONENT_RL_MODULE: old_learner_weights},
                    }
                )
                check(learner_group.get_weights(), old_learner_weights)

            # Check if we can load just single agent RL Modules.
            with tempfile.TemporaryDirectory() as tmpdir:
                module_0.save_to_path(tmpdir)
                with tempfile.TemporaryDirectory() as tmpdir2:
                    temp_module = spec.build()
                    temp_module.save_to_path(tmpdir2)

                    old_learner_weights = learner_group.get_weights()
                    learner_group.restore_from_path(
                        tmpdir,
                        component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/0",
                    )
                    learner_group.restore_from_path(
                        tmpdir2,
                        component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/1",
                    )
                    # check the weights of the module in the learner group are the
                    # same as the weights of the newly created marl module
                    new_marl_module = MultiAgentRLModule()
                    new_marl_module.add_module(module_id="0", module=module_0)
                    new_marl_module.add_module(module_id="1", module=temp_module)
                    check(learner_group.get_weights(), new_marl_module.get_state())
                    learner_group.set_weights(old_learner_weights)

            # Check if we can first load a MARLModule, then a single agent RLModule
            # (within that MARLModule). Check that the single agent RL Module is loaded
            # over the matching submodule in the MARL Module.
            with tempfile.TemporaryDirectory() as tmpdir:
                module_0 = spec.build()
                marl_module = MultiAgentRLModule()
                marl_module.add_module(module_id="0", module=module_0)
                marl_module.add_module(module_id="1", module=spec.build())
                marl_module.save_to_path(tmpdir)
                with tempfile.TemporaryDirectory() as tmpdir2:
                    module_1 = spec.build()
                    module_1.save_to_path(tmpdir2)
                    learner_group.restore_from_path(
                        tmpdir,
                        component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE,
                    )
                    learner_group.restore_from_path(
                        tmpdir2,
                        component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/1",
                    )
                    new_marl_module = MultiAgentRLModule()
                    new_marl_module.add_module(module_id="0", module=module_0)
                    new_marl_module.add_module(module_id="1", module=module_1)
                    check(learner_group.get_weights(), new_marl_module.get_state())
            del learner_group


class TestLearnerGroupSaveLoadState(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_save_to_path_and_restore_from_path(self):
        """Check that saving and loading learner group state works."""
        fws = ["torch", "tf2"]
        # this is expanded to more scaling modes on the release ci.
        scaling_modes = ["local-cpu", "multi-gpu-ddp"]
        test_iterator = itertools.product(fws, scaling_modes)
        batch = SampleBatch(FAKE_BATCH)
        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")

            config_overrides = REMOTE_CONFIGS.get(scaling_mode) or LOCAL_CONFIGS.get(
                scaling_mode
            )
            config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)
            learner_group = config.build_learner_group(env=env)

            # Checkpoint the initial learner state for later comparison.
            initial_learner_checkpoint_dir = tempfile.TemporaryDirectory().name
            learner_group.save_to_path(initial_learner_checkpoint_dir)
            # Test the convenience method `.get_weights()`.
            initial_weights = learner_group.get_weights()

            # Do a single update.
            learner_group.update_from_batch(batch.as_multi_agent())
            # Weights after the update must be different from original ones.
            check(
                initial_weights,
                learner_group.get_state(
                    components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE
                )[COMPONENT_LEARNER][COMPONENT_RL_MODULE],
                false=True,
            )

            # Checkpoint the learner state after 1 update for later comparison.
            learner_after_1_update_checkpoint_dir = tempfile.TemporaryDirectory().name
            learner_group.save_to_path(learner_after_1_update_checkpoint_dir)

            # Remove that learner, construct a new one, and load the state of the old
            # learner into the new one.
            learner_group.shutdown()
            del learner_group

            learner_group = config.build_learner_group(env=env)
            learner_group.restore_from_path(learner_after_1_update_checkpoint_dir)

            # Do another update.
            results_2nd_update_with_break = learner_group.update_from_batch(
                batch=batch.as_multi_agent()
            )
            weights_after_2_updates_with_break = learner_group.get_state(
                components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE
            )[COMPONENT_LEARNER][COMPONENT_RL_MODULE]
            learner_group.shutdown()
            del learner_group

            # Construct a new learner group and load the initial state of the learner.
            learner_group = config.build_learner_group(env=env)
            learner_group.restore_from_path(initial_learner_checkpoint_dir)
            check(
                initial_weights,
                learner_group.get_state(
                    components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE
                )[COMPONENT_LEARNER][COMPONENT_RL_MODULE],
            )
            # Perform 2 updates to get to the same state as the previous learners.
            learner_group.update_from_batch(batch.as_multi_agent())
            results_2nd_without_break = learner_group.update_from_batch(
                batch=batch.as_multi_agent()
            )
            weights_after_2_updates_without_break = learner_group.get_weights()
            learner_group.shutdown()
            del learner_group

            # Compare the results of the two updates.
            check(results_2nd_update_with_break, results_2nd_without_break)
            check(
                weights_after_2_updates_with_break,
                weights_after_2_updates_without_break,
                rtol=0.05,
            )


class TestLearnerGroupAsyncUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_async_update(self):
        """Test that async style updates converge to the same result as sync."""
        fws = ["torch", "tf2"]
        # async_update only needs to be tested for the most complex case.
        # so we'll only test it for multi-gpu-ddp.
        scaling_modes = ["multi-gpu-ddp", "remote-gpu"]
        test_iterator = itertools.product(fws, scaling_modes)

        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")
            config_overrides = REMOTE_CONFIGS[scaling_mode]
            config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)
            learner_group = config.build_learner_group(env=env)
            reader = get_cartpole_dataset_reader(batch_size=512)
            batch = reader.next()
            timer_sync = _Timer()
            timer_async = _Timer()
            with timer_sync:
                learner_group.update_from_batch(
                    batch=batch.as_multi_agent(), async_update=False
                )
            with timer_async:
                result_async = learner_group.update_from_batch(
                    batch=batch.as_multi_agent(), async_update=True
                )
            # ideally the the first async update will return nothing, and an easy
            # way to check that is if the time for an async update call is faster
            # than the time for a sync update call.
            self.assertLess(timer_async.mean, timer_sync.mean)
            self.assertIsInstance(result_async, dict)
            iter_i = 0
            while True:
                batch = reader.next()
                async_results = learner_group.update_from_batch(
                    batch.as_multi_agent(), async_update=True
                )
                if not async_results:
                    continue
                loss = async_results[ALL_MODULES][Learner.TOTAL_LOSS_KEY]
                # The loss is initially around 0.69 (ln2). When it gets to around
                # 0.57 the return of the policy gets to around 100.
                if loss < 0.57:
                    break
                # Compare reported "mean_weight" with actual ones.
                # TODO (sven): Right now, we don't have any way to know, whether
                #  an async update result came from the most recent call to
                #  `learner_group.update_from_batch(async_update=True)` or an earlier
                #  one. Once APPO/IMPALA are properly implemented on the new API stack,
                #  this problem should be resolved and we can uncomment the below line.
                # _check_multi_worker_weights(learner_group, async_results)
                iter_i += 1
            learner_group.shutdown()
            self.assertLess(loss, 0.57)


def _check_multi_worker_weights(learner_group, results):
    # Check that module weights are updated across workers and synchronized.
    # for i in range(1, len(results)):
    for module_id, mod_results in results.items():
        if module_id == ALL_MODULES:
            continue
        # Compare the reported mean weights (merged across all Learner workers,
        # which all should have the same weights after updating) with the actual
        # current mean weights.
        reported_mean_weights = mod_results["mean_weight"]
        parameters = learner_group.get_state(
            components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/" + module_id,
        )[COMPONENT_LEARNER][COMPONENT_RL_MODULE][module_id]
        actual_mean_weights = np.mean([w.mean() for w in parameters.values()])
        check(reported_mean_weights, actual_mean_weights, rtol=0.02)


if __name__ == "__main__":
    import sys

    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
