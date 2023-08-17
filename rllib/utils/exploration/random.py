from gymnasium.spaces import Discrete, Box, MultiDiscrete, Space
import numpy as np
import tree  # pip install dm_tree
from typing import Union, Optional

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils import force_tuple
from ray.rllib.utils.framework import try_import_tf, try_import_torch, TensorType
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.tf_utils import zero_logps_from_actions

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


@PublicAPI
class Random(Exploration):
    """A random action selector (deterministic/greedy for explore=False).

    If explore=True, returns actions randomly from `self.action_space` (via
    Space.sample()).
    If explore=False, returns the greedy/max-likelihood action.
    """

    def __init__(
        self, action_space: Space, *, model: ModelV2, framework: Optional[str], **kwargs
    ):
        """Initialize a Random Exploration object.

        Args:
            action_space: The gym action space used by the environment.
            framework: One of None, "tf", "torch".
        """
        super().__init__(
            action_space=action_space, model=model, framework=framework, **kwargs
        )

        self.action_space_struct = get_base_struct_from_space(self.action_space)

    @override(Exploration)
    def get_exploration_action(
        self,
        *,
        action_distribution: ActionDistribution,
        timestep: Union[int, TensorType],
        explore: bool = True
    ):
        # Instantiate the distribution object.
        if self.framework in ["tf2", "tf"]:
            return self.get_tf_exploration_action_op(action_distribution, explore)
        else:
            return self.get_torch_exploration_action(action_distribution, explore)

    def get_tf_exploration_action_op(
        self,
        action_dist: ActionDistribution,
        explore: Optional[Union[bool, TensorType]],
    ):
        def true_fn():
            batch_size = 1
            req = force_tuple(
                action_dist.required_model_output_shape(
                    self.action_space, getattr(self.model, "model_config", None)
                )
            )
            # Add a batch dimension?
            if len(action_dist.inputs.shape) == len(req) + 1:
                batch_size = tf.shape(action_dist.inputs)[0]

            # Function to produce random samples from primitive space
            # components: (Multi)Discrete or Box.
            def random_component(component):
                # Have at least an additional shape of (1,), even if the
                # component is Box(-1.0, 1.0, shape=()).
                shape = component.shape or (1,)

                if isinstance(component, Discrete):
                    return tf.random.uniform(
                        shape=(batch_size,) + component.shape,
                        maxval=component.n,
                        dtype=component.dtype,
                    )
                elif isinstance(component, MultiDiscrete):
                    return tf.concat(
                        [
                            tf.random.uniform(
                                shape=(batch_size, 1), maxval=n, dtype=component.dtype
                            )
                            for n in component.nvec
                        ],
                        axis=1,
                    )
                elif isinstance(component, Box):
                    if component.bounded_above.all() and component.bounded_below.all():
                        if component.dtype.name.startswith("int"):
                            return tf.random.uniform(
                                shape=(batch_size,) + shape,
                                minval=component.low.flat[0],
                                maxval=component.high.flat[0],
                                dtype=component.dtype,
                            )
                        else:
                            return tf.random.uniform(
                                shape=(batch_size,) + shape,
                                minval=component.low,
                                maxval=component.high,
                                dtype=component.dtype,
                            )
                    else:
                        return tf.random.normal(
                            shape=(batch_size,) + shape, dtype=component.dtype
                        )
                else:
                    assert isinstance(component, Simplex), (
                        "Unsupported distribution component '{}' for random "
                        "sampling!".format(component)
                    )
                    return tf.nn.softmax(
                        tf.random.uniform(
                            shape=(batch_size,) + shape,
                            minval=0.0,
                            maxval=1.0,
                            dtype=component.dtype,
                        )
                    )

            actions = tree.map_structure(random_component, self.action_space_struct)
            return actions

        def false_fn():
            return action_dist.deterministic_sample()

        action = tf.cond(
            pred=tf.constant(explore, dtype=tf.bool)
            if isinstance(explore, bool)
            else explore,
            true_fn=true_fn,
            false_fn=false_fn,
        )

        logp = zero_logps_from_actions(action)
        return action, logp

    def get_torch_exploration_action(
        self, action_dist: ActionDistribution, explore: bool
    ):
        if explore:
            req = force_tuple(
                action_dist.required_model_output_shape(
                    self.action_space, getattr(self.model, "model_config", None)
                )
            )
            # Add a batch dimension?
            if len(action_dist.inputs.shape) == len(req) + 1:
                batch_size = action_dist.inputs.shape[0]
                a = np.stack([self.action_space.sample() for _ in range(batch_size)])
            else:
                a = self.action_space.sample()
            # Convert action to torch tensor.
            action = torch.from_numpy(a).to(self.device)
        else:
            action = action_dist.deterministic_sample()
        logp = torch.zeros((action.size()[0],), dtype=torch.float32, device=self.device)
        return action, logp
