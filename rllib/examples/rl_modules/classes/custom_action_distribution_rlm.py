from typing import Any, Dict, Optional

from ray.rllib.core.columns import Columns
from ray.rllib.core.distribution.torch.torch_distribution import TorchCategorical
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


def _make_categorical_with_temperature(temp):
    """Helper function to create a new action distribution class.

    The returned class takes a temperature parameter in its constructor with the default
    value `temp`.

    Args:
        temp: The default temperature to use for the generated distribution class.
    """

    class TorchCategoricalWithTemp(TorchCategorical):
        def __init__(self, logits=None, probs=None, temperature: float = temp):
            """Initializes a TorchCategoricalWithTemp instance.

            Args:
                logits: Event log probabilities (non-normalized).
                probs: The probabilities of each event.
                temperature: In case of using logits, this parameter can be used to
                    determine the sharpness of the distribution. i.e.
                    ``probs = softmax(logits / temperature)``. The temperature must be
                    strictly positive. A low value (e.g. 1e-10) will result in argmax
                    sampling while a larger value will result in uniform sampling.
            """
            # Either divide logits or probs by the temperature.
            assert (
                temperature > 0.0
            ), f"Temperature ({temperature}) must be strictly positive!"
            if logits is not None:
                logits /= temperature
            else:
                probs = torch.nn.functional.softmax(probs / temperature)
            super().__init__(logits, probs)

    return TorchCategoricalWithTemp


class CustomActionDistributionRLModule(TorchRLModule, ValueFunctionAPI):
    """A simple TorchRLModule with its own custom action distribution.

    The distribution differs from the default one by an additional temperature
    parameter applied on top of the Categorical base distribution. See the above
    `TorchCategoricalWithTemp` class for details.

    .. testcode::

        import numpy as np
        import gymnasium as gym

        my_net = CustomActionDistributionRLModule(
            observation_space=gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
            action_space=gym.spaces.Discrete(4),
            model_config={"action_dist_temperature": 5.0},
        )

        B = 10
        data = torch.from_numpy(
            np.random.random_sample(size=(B, 4)).astype(np.float32)
        )
        # Expect a relatively high-temperature distribution.
        # Set "action_dist_temperature" to small values << 1.0 to approximate greedy
        # behavior (even when stochastically sampling from the distribution).
        print(my_net.forward_exploration({"obs": data}))
    """

    @override(TorchRLModule)
    def setup(self):
        """Use this method to create all the model components that you require.

        Feel free to access the following useful properties in this class:
        - `self.model_config`: The config dict for this RLModule class,
        which should contain flexible settings, for example: {"hiddens": [256, 256]}.
        - `self.observation|action_space`: The observation and action space that
        this RLModule is subject to. Note that the observation space might not be the
        exact space from your env, but that it might have already gone through
        preprocessing through a connector pipeline (for example, flattening,
        frame-stacking, mean/std-filtering, etc..).
        - `self.inference_only`: If True, this model should be built only for inference
        purposes, in which case you may want to exclude any components that are not used
        for computing actions, for example a value function branch.
        """
        input_dim = self.observation_space.shape[0]
        hidden_dim = self.model_config.get("hidden_dim", 256)
        output_dim = self.action_space.n

        # Define simple encoder, and policy- and vf heads.
        self._encoder = torch.nn.Sequential(
            torch.nn.Linear(input_dim, hidden_dim),
            torch.nn.ReLU(),
        )
        self._policy_net = torch.nn.Linear(hidden_dim, output_dim)
        self._vf = nn.Linear(hidden_dim, 1)

        # Plug in a custom action dist class.
        # NOTE: If you need more granularity as to which distribution class is used by
        # which forward method (`forward_inference`, `forward_exploration`,
        # `forward_train`), override the RLModule methods
        # `get_inference_action_dist_cls`, `get_exploration_action_dist_cls`, and
        # `get_train_action_dist_cls`, and return
        # your custom class(es) from these. In this case, leave `self.action_dist_cls`
        # set to None, its default value.
        self.action_dist_cls = _make_categorical_with_temperature(
            self.model_config["action_dist_temperature"]
        )

    @override(TorchRLModule)
    def _forward(self, batch, **kwargs):
        # Compute the basic 1D feature tensor (inputs to policy- and value-heads).
        _, logits = self._compute_embeddings_and_logits(batch)
        # Return features and logits as ACTION_DIST_INPUTS (categorical distribution).
        return {
            Columns.ACTION_DIST_INPUTS: logits,
        }

    @override(TorchRLModule)
    def _forward_train(self, batch, **kwargs):
        # Compute the basic 1D feature tensor (inputs to policy- and value-heads).
        embeddings, logits = self._compute_embeddings_and_logits(batch)
        # Return features and logits as ACTION_DIST_INPUTS (categorical distribution).
        return {
            Columns.ACTION_DIST_INPUTS: logits,
            Columns.EMBEDDINGS: embeddings,
        }

    # We implement this RLModule as a ValueFunctionAPI RLModule, so it can be used
    # by value-based methods like PPO or IMPALA.
    @override(ValueFunctionAPI)
    def compute_values(
        self,
        batch: Dict[str, Any],
        embeddings: Optional[Any] = None,
    ) -> TensorType:
        # Features not provided -> We need to compute them first.
        if embeddings is None:
            embeddings = self._encoder(batch[Columns.OBS])
        return self._vf(embeddings).squeeze(-1)

    def _compute_embeddings_and_logits(self, batch):
        embeddings = self._encoder(batch[Columns.OBS])
        logits = self._policy_net(embeddings)
        return embeddings, logits
