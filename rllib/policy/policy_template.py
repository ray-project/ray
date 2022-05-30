from typing import (
    TYPE_CHECKING,
)

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.jax.jax_modelv2 import JAXModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils import add_mixins, NullContextManager
from ray.rllib.utils.annotations import Deprecated, override
from ray.rllib.utils.framework import try_import_torch, try_import_jax
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.numpy import convert_to_numpy

if TYPE_CHECKING:
    from ray.rllib.evaluation.episode import Episode  # noqa

jax, _ = try_import_jax()
torch, _ = try_import_torch()


@Deprecated(
    new="sub-class directly from `ray.rllib.policy.torch_policy_v2::TorchPolicyV2` "
    "and override needed methods",
    error=False,
)
def build_policy_class(
    name: str,
    framework: str,
    *,
    loss_fn=None,
    get_default_config=None,
    stats_fn=None,
    postprocess_fn=None,
    extra_action_out_fn=None,
    extra_grad_process_fn=None,
    extra_learn_fetches_fn=None,
    optimizer_fn=None,
    validate_spaces=None,
    before_init=None,
    before_loss_init=None,
    after_init=None,
    _after_loss_init=None,
    action_sampler_fn=None,
    action_distribution_fn=None,
    make_model=None,
    make_model_and_action_dist=None,
    compute_gradients_fn=None,
    apply_gradients_fn=None,
    mixins=None,
    get_batch_divisibility_req=None,
):
    original_kwargs = locals().copy()
    parent_cls = TorchPolicy
    base = add_mixins(parent_cls, mixins)

    class policy_cls(base):
        def __init__(self, obs_space, action_space, config):
            # Set up the config from possible default-config fn and given
            # config arg.
            if get_default_config:
                config = dict(get_default_config(), **config)
            self.config = config

            # Set the DL framework for this Policy.
            self.framework = self.config["framework"] = framework

            # Validate observation- and action-spaces.
            if validate_spaces:
                validate_spaces(self, obs_space, action_space, self.config)

            # Do some pre-initialization steps.
            if before_init:
                before_init(self, obs_space, action_space, self.config)

            # Model is customized (use default action dist class).
            if make_model:
                assert make_model_and_action_dist is None, (
                    "Either `make_model` or `make_model_and_action_dist`"
                    " must be None!"
                )
                self.model = make_model(self, obs_space, action_space, config)
                dist_class, _ = ModelCatalog.get_action_dist(
                    action_space, self.config["model"], framework=framework
                )
            # Model and action dist class are customized.
            elif make_model_and_action_dist:
                self.model, dist_class = make_model_and_action_dist(
                    self, obs_space, action_space, config
                )
            # Use default model and default action dist.
            else:
                dist_class, logit_dim = ModelCatalog.get_action_dist(
                    action_space, self.config["model"], framework=framework
                )
                self.model = ModelCatalog.get_model_v2(
                    obs_space=obs_space,
                    action_space=action_space,
                    num_outputs=logit_dim,
                    model_config=self.config["model"],
                    framework=framework,
                )

            # Make sure, we passed in a correct Model factory.
            model_cls = TorchModelV2 if framework == "torch" else JAXModelV2
            assert isinstance(
                self.model, model_cls
            ), "ERROR: Generated Model must be a TorchModelV2 object!"

            # Call the framework-specific Policy constructor.
            self.parent_cls = parent_cls
            self.parent_cls.__init__(
                self,
                observation_space=obs_space,
                action_space=action_space,
                config=config,
                model=self.model,
                loss=None if self.config["in_evaluation"] else loss_fn,
                action_distribution_class=dist_class,
                action_sampler_fn=action_sampler_fn,
                action_distribution_fn=action_distribution_fn,
                max_seq_len=config["model"]["max_seq_len"],
                get_batch_divisibility_req=get_batch_divisibility_req,
            )

            # Merge Model's view requirements into Policy's.
            self.view_requirements.update(self.model.view_requirements)

            _before_loss_init = before_loss_init or after_init
            if _before_loss_init:
                _before_loss_init(
                    self, self.observation_space, self.action_space, config
                )

            # Perform test runs through postprocessing- and loss functions.
            self._initialize_loss_from_dummy_batch(
                auto_remove_unneeded_view_reqs=True,
                stats_fn=None if self.config["in_evaluation"] else stats_fn,
            )

            if _after_loss_init:
                _after_loss_init(self, obs_space, action_space, config)

            # Got to reset global_timestep again after this fake run-through.
            self.global_timestep = 0

        @override(Policy)
        def postprocess_trajectory(
            self, sample_batch, other_agent_batches=None, episode=None
        ):
            # Do all post-processing always with no_grad().
            # Not using this here will introduce a memory leak
            # in torch (issue #6962).
            with self._no_grad_context():
                # Call super's postprocess_trajectory first.
                sample_batch = super().postprocess_trajectory(
                    sample_batch, other_agent_batches, episode
                )
                if postprocess_fn:
                    return postprocess_fn(
                        self, sample_batch, other_agent_batches, episode
                    )

                return sample_batch

        @override(parent_cls)
        def extra_grad_process(self, optimizer, loss):
            """Called after optimizer.zero_grad() and loss.backward() calls.

            Allows for gradient processing before optimizer.step() is called.
            E.g. for gradient clipping.
            """
            if extra_grad_process_fn:
                return extra_grad_process_fn(self, optimizer, loss)
            else:
                return parent_cls.extra_grad_process(self, optimizer, loss)

        @override(parent_cls)
        def extra_compute_grad_fetches(self):
            if extra_learn_fetches_fn:
                fetches = convert_to_numpy(extra_learn_fetches_fn(self))
                # Auto-add empty learner stats dict if needed.
                return dict({LEARNER_STATS_KEY: {}}, **fetches)
            else:
                return parent_cls.extra_compute_grad_fetches(self)

        @override(parent_cls)
        def compute_gradients(self, batch):
            if compute_gradients_fn:
                return compute_gradients_fn(self, batch)
            else:
                return parent_cls.compute_gradients(self, batch)

        @override(parent_cls)
        def apply_gradients(self, gradients):
            if apply_gradients_fn:
                apply_gradients_fn(self, gradients)
            else:
                parent_cls.apply_gradients(self, gradients)

        @override(parent_cls)
        def extra_action_out(self, input_dict, state_batches, model, action_dist):
            with self._no_grad_context():
                if extra_action_out_fn:
                    stats_dict = extra_action_out_fn(
                        self, input_dict, state_batches, model, action_dist
                    )
                else:
                    stats_dict = parent_cls.extra_action_out(
                        self, input_dict, state_batches, model, action_dist
                    )
                return self._convert_to_numpy(stats_dict)

        @override(parent_cls)
        def optimizer(self):
            if optimizer_fn:
                optimizers = optimizer_fn(self, self.config)
            else:
                optimizers = parent_cls.optimizer(self)
            return optimizers

        @override(parent_cls)
        def extra_grad_info(self, train_batch):
            with self._no_grad_context():
                if stats_fn:
                    stats_dict = stats_fn(self, train_batch)
                else:
                    stats_dict = self.parent_cls.extra_grad_info(self, train_batch)
                return self._convert_to_numpy(stats_dict)

        def _no_grad_context(self):
            if self.framework == "torch":
                return torch.no_grad()
            return NullContextManager()

        def _convert_to_numpy(self, data):
            if self.framework == "torch":
                return convert_to_numpy(data)
            return data

    def with_updates(**overrides):
        """Creates a Torch|JAXPolicy cls based on settings of another one.

        Keyword Args:
            **overrides: The settings (passed into `build_torch_policy`) that
                should be different from the class that this method is called
                on.

        Returns:
            type: A new Torch|JAXPolicy sub-class.

        Examples:
        >> MySpecialDQNPolicyClass = DQNTorchPolicy.with_updates(
        ..    name="MySpecialDQNPolicyClass",
        ..    loss_function=[some_new_loss_function],
        .. )
        """
        return build_policy_class(**dict(original_kwargs, **overrides))

    policy_cls.with_updates = staticmethod(with_updates)
    policy_cls.__name__ = name
    policy_cls.__qualname__ = name
    return policy_cls
