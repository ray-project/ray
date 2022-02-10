import gym
from typing import Callable, Dict, List, Optional, Tuple, Type, Union

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModelGradients, TensorType, TrainerConfigDict

torch, _ = try_import_torch()


@Deprecated(new="build_policy_class(framework='torch')", error=False)
def build_torch_policy(
    name: str,
    *,
    loss_fn: Optional[
        Callable[
            [Policy, ModelV2, Type[TorchDistributionWrapper], SampleBatch],
            Union[TensorType, List[TensorType]],
        ]
    ],
    get_default_config: Optional[Callable[[], TrainerConfigDict]] = None,
    stats_fn: Optional[Callable[[Policy, SampleBatch], Dict[str, TensorType]]] = None,
    postprocess_fn=None,
    extra_action_out_fn: Optional[
        Callable[
            [
                Policy,
                Dict[str, TensorType],
                List[TensorType],
                ModelV2,
                TorchDistributionWrapper,
            ],
            Dict[str, TensorType],
        ]
    ] = None,
    extra_grad_process_fn: Optional[
        Callable[[Policy, "torch.optim.Optimizer", TensorType], Dict[str, TensorType]]
    ] = None,
    extra_learn_fetches_fn: Optional[Callable[[Policy], Dict[str, TensorType]]] = None,
    optimizer_fn: Optional[
        Callable[[Policy, TrainerConfigDict], "torch.optim.Optimizer"]
    ] = None,
    validate_spaces: Optional[
        Callable[[Policy, gym.Space, gym.Space, TrainerConfigDict], None]
    ] = None,
    before_init: Optional[
        Callable[[Policy, gym.Space, gym.Space, TrainerConfigDict], None]
    ] = None,
    before_loss_init: Optional[
        Callable[[Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict], None]
    ] = None,
    after_init: Optional[
        Callable[[Policy, gym.Space, gym.Space, TrainerConfigDict], None]
    ] = None,
    _after_loss_init: Optional[
        Callable[[Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict], None]
    ] = None,
    action_sampler_fn: Optional[
        Callable[[TensorType, List[TensorType]], Tuple[TensorType, TensorType]]
    ] = None,
    action_distribution_fn: Optional[
        Callable[
            [Policy, ModelV2, TensorType, TensorType, TensorType],
            Tuple[TensorType, type, List[TensorType]],
        ]
    ] = None,
    make_model: Optional[
        Callable[
            [Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict], ModelV2
        ]
    ] = None,
    make_model_and_action_dist: Optional[
        Callable[
            [Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict],
            Tuple[ModelV2, Type[TorchDistributionWrapper]],
        ]
    ] = None,
    compute_gradients_fn: Optional[
        Callable[[Policy, SampleBatch], Tuple[ModelGradients, dict]]
    ] = None,
    apply_gradients_fn: Optional[
        Callable[[Policy, "torch.optim.Optimizer"], None]
    ] = None,
    mixins: Optional[List[type]] = None,
    get_batch_divisibility_req: Optional[Callable[[Policy], int]] = None
) -> Type[TorchPolicy]:

    kwargs = locals().copy()
    # Set to torch and call new function.
    kwargs["framework"] = "torch"
    return build_policy_class(**kwargs)
