"""Example of training a custom RLModule with BC first, then finetuning it with PPO.

This example:
    - demonstrates how to write a very simple custom BC RLModule.
    - run a quick BC training experiment with the custom module and learn CartPole
    until some episode return A, while checkpointing each iteration.
    - shows how subclass the custom BC RLModule, add the ValueFunctionAPI to the
    new class, and add a value-function branch and an implementation of
    `compute_values` to the original model to make it work with a value-based algo
    like PPO.
    - shows how to plug this new PPO-capable RLModule (including its checkpointed state
    from the BC run) into your algorithm's config.
    - confirms that even after 1-2 training iterations with PPO, no catastrophic
    forgetting occurs (due to the additional value function branch and the switched
    optimizer).
    - uses Tune and RLlib to continue training the model until a higher return of B
    is reached.


How to run this script
----------------------
`python [script file name].py`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can first see BC's performance until return A is reached:
+----------------------------+------------+----------------+--------+
| Trial name                 | status     | loc            |   iter |
|                            |            |                |        |
|----------------------------+------------+----------------+--------+
| BC_CartPole-v1_95ba0_00000 | TERMINATED | 127.0.0.1:1515 |     51 |
+----------------------------+------------+----------------+--------+
+------------------+------------------------+------------------------+
|   total time (s) |    episode_return_mean |   num_env_steps_traine |
|                  |                        |             d_lifetime |
|------------------+------------------------|------------------------|
|          11.4828 |                  250.5 |                  42394 |
+------------------+------------------------+------------------------+

The script should confirm that no catastrophic forgetting has taken place:

PPO return after initialization: 292.3
PPO return after 2x training: 276.85

Then, after PPO training, you should see something like this (higher return):
+-----------------------------+------------+----------------+--------+
| Trial name                  | status     | loc            |   iter |
|                             |            |                |        |
|-----------------------------+------------+----------------+--------+
| PPO_CartPole-v1_e07ac_00000 | TERMINATED | 127.0.0.1:6032 |     37 |
+-----------------------------+------------+----------------+--------+

+------------------+------------------------+------------------------+
|   total time (s) |    episode_return_mean |  num_episodes_lifetime |
|                  |                        |                        |
+------------------+------------------------+------------------------+
|          32.7647 |                 450.76 |                    406 |
+------------------+------------------------+------------------------+
"""
from pathlib import Path

from torch import nn

from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import (
    COMPONENT_LEARNER,
    COMPONENT_LEARNER_GROUP,
    COMPONENT_RL_MODULE,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import MLPEncoderConfig, MLPHeadConfig
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
)

parser = add_rllib_example_script_args()
parser.set_defaults(
    env="CartPole-v1",
    checkpoint_freq=1,
)


class MyBCModel(TorchRLModule):
    """A very simple BC-usable model that only computes action logits."""

    @override(TorchRLModule)
    def setup(self):
        # Create an encoder trunk.
        # Observations are directly passed through it and feature vectors are output.
        self._encoder = MLPEncoderConfig(
            input_dims=[4],  # CartPole
            hidden_layer_dims=[256, 256],
            hidden_layer_activation="relu",
            output_layer_dim=None,
        ).build(framework="torch")

        # The policy head sitting on top of the encoder. Feature vectors come in as
        # input and action logits are output.
        self._pi = MLPHeadConfig(
            input_dims=[256],  # from encoder
            hidden_layer_dims=[256],  # pi head
            hidden_layer_activation="relu",
            output_layer_dim=2,  # CartPole
            output_layer_activation="linear",
        ).build(framework="torch")

    @override(TorchRLModule)
    def _forward_inference(self, batch, **kwargs):
        return {Columns.ACTION_DIST_INPUTS: self._pi(self._encoder(batch)[ENCODER_OUT])}

    @override(RLModule)
    def _forward_exploration(self, batch, **kwargs):
        return self._forward_inference(batch)

    @override(RLModule)
    def _forward_train(self, batch, **kwargs):
        return self._forward_inference(batch)


class MyPPOModel(MyBCModel, ValueFunctionAPI):
    """Subclass of our simple BC model, but implementing the ValueFunctionAPI.

    Implementing the `compute_values` method makes this RLModule usable by algos
    like PPO.
    """

    @override(MyBCModel)
    def setup(self):
        # Call super setup to create encoder trunk and policy head.
        super().setup()
        # Create the new value function head and zero-initialize it to not cause too
        # much disruption.
        self._vf = MLPHeadConfig(
            input_dims=[256],  # from encoder
            hidden_layer_dims=[256],  # pi head
            hidden_layer_activation="relu",
            hidden_layer_weights_initializer=nn.init.zeros_,
            hidden_layer_bias_initializer=nn.init.zeros_,
            output_layer_dim=1,  # 1=value node
            output_layer_activation="linear",
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
    def compute_values(self, batch, embeddings=None):
        # Compute embeddings ...
        if embeddings is None:
            embeddings = self._encoder(batch)[ENCODER_OUT]
        # then values using our value head.
        return self._vf(embeddings).squeeze(-1)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.env == "CartPole-v1", "This example works only with --env=CartPole-v1!"

    # Define the data paths for our CartPole large dataset.
    base_path = Path(__file__).parents[2]
    assert base_path.is_dir(), base_path
    data_path = base_path / "offline/tests/data/cartpole/cartpole-v1_large"
    assert data_path.is_dir(), data_path
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
            input_read_method_kwargs={
                "override_num_blocks": max((args.num_learners or 1) * 2, 2)
            },
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
            # iteration. For single-learner mode, 1 is the only option.
            dataset_num_iters_per_learner=1 if not args.num_learners else None,
        ).training(
            train_batch_size_per_learner=1024,
            # To increase learning speed with multiple learners,
            # increase the learning rate correspondingly.
            lr=0.0008 * (args.num_learners or 1) ** 0.5,
        )
        # Plug in our simple custom BC model from above.
        .rl_module(rl_module_spec=RLModuleSpec(module_class=MyBCModel))
        # Run evaluation to observe how good our BC policy already is.
        .evaluation(
            evaluation_interval=3,
            evaluation_num_env_runners=1,
            evaluation_duration=5,
            evaluation_parallel_to_training=True,
        )
    )

    # Run the BC experiment and stop at R=250.0
    metric_key = f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    stop = {metric_key: 250.0}
    results = run_rllib_example_script_experiment(base_config, args, stop=stop)

    # Extract the RLModule checkpoint.
    best_result = results.get_best_result(metric_key)
    rl_module_checkpoint = (
        Path(best_result.checkpoint.path)
        / COMPONENT_LEARNER_GROUP
        / COMPONENT_LEARNER
        / COMPONENT_RL_MODULE
        / "default_policy"
    )

    # Create a new PPO config.
    base_config = (
        PPOConfig()
        .environment(args.env)
        .env_runners(create_env_on_local_worker=True)
        .training(
            # Keep lr relatively low at the beginning to avoid catastrophic forgetting.
            lr=0.00002,
            num_epochs=6,
            vf_loss_coeff=0.01,
        )
        # Plug in our simple custom PPO model from above. Note that the checkpoint
        # for the BC model is loadable into the PPO model, b/c the BC model is a subset
        # of the PPO model (all weights/biases in the BC model are also found in the PPO
        # model; the PPO model only has an additional value function branch).
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
    # Check, whether training 2 times causes catastrophic forgetting.
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
