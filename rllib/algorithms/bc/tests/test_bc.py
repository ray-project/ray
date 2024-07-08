import os
from pathlib import Path
import unittest

import ray
import ray.rllib.algorithms.bc as bc
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestBC(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_bc_compilation_and_learning_from_offline_file(self):
        """Test whether BC can be built with all frameworks.

        And learns from a historic-data file (while being evaluated on an
        actual env using evaluation_num_env_runners > 0).
        """
        rllib_dir = Path(__file__).parents[3]
        print("rllib_dir={}".format(rllib_dir))
        # This has still to be done until `pathlib` will be used in the readers.
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print(f"data_file={data_file} exists={os.path.isfile(data_file)}")

        config = (
            bc.BCConfig()
            .evaluation(
                evaluation_interval=3,
                evaluation_num_env_runners=1,
                evaluation_duration=5,
                evaluation_parallel_to_training=True,
                evaluation_config=bc.BCConfig.overrides(input_="sampler"),
            )
            .offline_data(input_=[data_file])
        )
        num_iterations = 350
        min_return_to_reach = 75.0

        # Test for RLModule API and ModelV2.
        for rl_modules in [True, False]:
            config.api_stack(enable_rl_module_and_learner=rl_modules)
            # Old and new stack support different frameworks
            if rl_modules:
                frameworks_to_test = ("torch", "tf2")
            else:
                frameworks_to_test = ("torch", "tf")

            for _ in framework_iterator(config, frameworks=frameworks_to_test):
                for recurrent in [True, False]:
                    # We only test recurrent networks with RLModules.
                    if recurrent:
                        # TODO (Artur): We read input data without a time-dimensions.
                        #  In order for a recurrent offline learning RL Module to
                        #  work, the input data needs to be transformed do add a
                        #  time-dimension.
                        continue

                    config.training(model={"use_lstm": recurrent})
                    algo = config.build(env="CartPole-v1")
                    learnt = False
                    for i in range(num_iterations):
                        results = algo.train()
                        check_train_results(results)
                        print(results)

                        eval_results = results.get("evaluation")
                        if eval_results:
                            mean_return = eval_results[ENV_RUNNER_RESULTS][
                                EPISODE_RETURN_MEAN
                            ]
                            print("iter={} R={}".format(i, mean_return))
                            # Learn until good reward is reached in the actual env.
                            if mean_return > min_return_to_reach:
                                print("learnt!")
                                learnt = True
                                break

                    if not learnt:
                        raise ValueError(
                            "`BC` did not reach {} reward from expert offline "
                            "data!".format(min_return_to_reach)
                        )

                    check_compute_single_action(algo, include_prev_action_reward=True)

                    algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
