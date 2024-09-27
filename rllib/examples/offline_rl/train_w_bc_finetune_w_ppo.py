from pathlib import Path

from torch import nn

from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import (
    COMPONENT_LEARNER_GROUP,
    COMPONENT_LEARNER,
    COMPONENT_RL_MODULE,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import MLPEncoderConfig, MLPHeadConfig
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args()
parser.set_defaults(
    enable_new_api_stack=True,
    env="CartPole-v1",
    checkpoint_freq=1,
)


class MyBCModel(TorchRLModule):
    @override(TorchRLModule)
    def setup(self):
        self._encoder = MLPEncoderConfig(
            input_dims=[4],  # CartPole
            hidden_layer_dims=[256, 256],
            hidden_layer_activation="relu",
            output_layer_dim=None,
        ).build(framework="torch")

        self._pi = MLPHeadConfig(
            input_dims=[256],  # from encoder
            hidden_layer_dims=[256],  # pi head
            hidden_layer_activation="relu",
            output_layer_dim=2,  # CartPole
            output_layer_activation="linear",
        ).build(framework="torch")

    @override(TorchRLModule)
    def _forward_inference(self, batch, **kwargs):
        return {
            Columns.ACTION_DIST_INPUTS: self._pi(self._encoder(batch)[ENCODER_OUT])
        }

    @override(RLModule)
    def _forward_exploration(self, batch, **kwargs):
        return self._forward_inference(batch)

    @override(RLModule)
    def _forward_train(self, batch, **kwargs):
        return self._forward_inference(batch)


class MyPPOModel(MyBCModel, ValueFunctionAPI):
    @override(MyBCModel)
    def setup(self):
        super().setup()
        self._vf = MLPHeadConfig(
            input_dims=[256],  # from encoder
            hidden_layer_dims=[256],  # pi head
            hidden_layer_activation="relu",
            # Zero-initialize value function to not cause too much disruption.
            hidden_layer_weights_initializer=nn.init.zeros_,
            hidden_layer_bias_initializer=nn.init.zeros_,
            output_layer_dim=1,  # value node
            output_layer_activation="linear",
            # Zero-initialize value function to not cause too much disruption.
            output_layer_weights_initializer=nn.init.zeros_,
            output_layer_bias_initializer=nn.init.zeros_,
        ).build(framework="torch")

    @override(MyBCModel)
    def _forward_train(self, batch, **kwargs):
        features = self._encoder(batch)[ENCODER_OUT]
        logits = self._pi(features)
        vf_out = self._vf(features).squeeze(-1)
        return {
            Columns.ACTION_DIST_INPUTS: logits,
            Columns.VF_PREDS: vf_out,
        }

    @override(ValueFunctionAPI)
    def compute_values(self, batch):
        features = self._encoder(batch)[ENCODER_OUT]
        return self._vf(features).squeeze(-1)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.env == "CartPole-v1", "This example works only with --env=CartPole-v1!"

    # Define the data paths for our CartPole large dataset.
    data_path = "tests/data/cartpole/cartpole-v1_large"
    base_path = Path(__file__).parents[2]
    print(f"base_path={base_path}")
    data_path = "local://" / base_path / data_path
    print(f"data_path={data_path}")

    # Define the BC config.
    base_config = (
        BCConfig()
        # Note, the `input_` argument is the major argument for the
        # new offline API. Via the `input_read_method_kwargs` the
        # arguments for the `ray.data.Dataset` read method can be
        # configured. The read method needs at least as many blocks
        # as remote learners.
        .offline_data(
            input_=[data_path.as_posix()],
            # Define the number of reading blocks, these should be larger than 1
            # and aligned with the data size.
            input_read_method_kwargs={"override_num_blocks": max(args.num_gpus * 2, 2)},
            # Concurrency defines the number of processes that run the
            # `map_batches` transformations. This should be aligned with the
            # 'prefetch_batches' argument in 'iter_batches_kwargs'.
            map_batches_kwargs={"concurrency": 2, "num_cpus": 2},
            # This data set is small so do not prefetch too many batches and use no
            # local shuffle.
            iter_batches_kwargs={
                "prefetch_batches": 1,
                "local_shuffle_buffer_size": None,
            },
            # The number of iterations to be run per learner when in multi-learner
            # mode in a single RLlib training iteration. Leave this to `None` to
            # run an entire epoch on the dataset during a single RLlib training
            # iteration. For single-learner mode 1 is the only option.
            dataset_num_iters_per_learner=1 if args.num_gpus == 0 else None,
        )
        .training(
            train_batch_size_per_learner=1024,
            # To increase learning speed with multiple learners,
            # increase the learning rate correspondingly.
            lr=0.0008 * max(1, args.num_gpus**0.5),
        )
        .rl_module(rl_module_spec=RLModuleSpec(
            module_class=MyBCModel,
        ))
        .evaluation(
            evaluation_interval=3,
            evaluation_num_env_runners=1,
            evaluation_duration=5,
            evaluation_parallel_to_training=True,
        )
    )

    metric_key = f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    stop = {metric_key: 250.0}

    results = run_rllib_example_script_experiment(base_config, args, stop=stop)

    # Extract the RLModule checkpoint.
    best_result = results.get_best_result(metric_key)
    rl_module_checkpoint = Path(best_result.checkpoint.path) / COMPONENT_LEARNER_GROUP / COMPONENT_LEARNER / COMPONENT_RL_MODULE / "default_policy"

    base_config = (
        PPOConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment(args.env)
        .training(
            # Keep lr relatively low to avoid catastrophic forgetting (but not
            # too low).
            lr=0.00002,
            num_epochs=6,
            vf_loss_coeff=0.01,
        )
        .rl_module(
            rl_module_spec=RLModuleSpec(
                module_class=MyPPOModel,
                load_state_path=rl_module_checkpoint,
            )
        )
    )

    # Quick test, whether initial performance in the loaded (now PPO) model is ok.
    ppo = base_config.build()
    eval_results = ppo.evaluate()
    R = eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
    assert R >= 200.0, f"Initial PPO performance bad! R={R} (expected 200.0+)."
    print(f"PPO return after initialization: {R}")

    ppo.train()
    train_results = ppo.train()
    R = train_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
    assert R >= 250.0, f"PPO performance (training) bad! R={R} (expected 250.0+)."
    print(f"PPO return after 2x training: {R}")

    # Perform actual PPO training run (this time until 450.0 return).
    stop = {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 450.0,
    }
    run_rllib_example_script_experiment(base_config, args, stop=stop)
