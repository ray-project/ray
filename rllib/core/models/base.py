import abc

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.configs import ModelConfig
from ray.rllib.policy.rnn_sequencing import get_fold_unfold_fns
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.util.annotations import DeveloperAPI

# Top level keys that unify model i/o.
ENCODER_OUT: str = "encoder_out"
# For Actor-Critic algorithms, these signify data related to the actor and critic
ACTOR: str = "actor"
CRITIC: str = "critic"


@DeveloperAPI(stability="alpha")
class Encoder(abc.ABC):
    """The framework-agnostic base class for all RLlib encoders.

    Encoders are used to transform observations to a latent space.
    Therefore, their `input_specs` contains the observation space dimensions.
    Similarly, their `output_specs` contains the latent space dimensions.
    Encoders can be recurrent, in which case the state should be part of input- and
    output_specs. The latent vectors produced by an encoder are fed into subsequent
    "heads". Any implementation of Encoder should also be callable. This should be done
    by also inheriting from a framework-specific model base-class, s.a. TorchModel or
    TfModel.

    Abstract illustration of typical flow of tensors:

    Inputs
    |
    Encoder
    |      \
    Head1  Head2
    |      /
    Outputs

    Outputs of encoders are generally of shape (B, latent_dim) or (B, T, latent_dim).
    That is, for time-series data, we encode into the latent space for each time step.
    This should be reflected in the `output_specs`.

    Usage example together with a ModelConfig:

    .. testcode::

        from dataclasses import dataclass
        import numpy as np

        from ray.rllib.core.columns import Columns
        from ray.rllib.core.models.base import Encoder, ENCODER_OUT
        from ray.rllib.core.models.configs import ModelConfig
        from ray.rllib.policy.sample_batch import SampleBatch

        class NumpyEncoder(Encoder):
            def __init__(self, config):
                super().__init__(config)
                self.factor = config.factor

            def __call__(self, *args, **kwargs):
                # This is a dummy method to do checked forward passes.
                return self._forward(*args, **kwargs)

            def _forward(self, input_dict, **kwargs):
                obs = input_dict[Columns.OBS]
                return {
                    ENCODER_OUT: np.array(obs) * self.factor,
                    Columns.STATE_OUT: (
                        np.array(input_dict[Columns.STATE_IN])
                        * self.factor
                    ),
                }

        @dataclass
        class NumpyEncoderConfig(ModelConfig):
            factor: int = None

            def build(self, framework: str):
                return NumpyEncoder(self)

        config = NumpyEncoderConfig(factor=2)
        encoder = NumpyEncoder(config)
        print(encoder({Columns.OBS: 1, Columns.STATE_IN: 2}))

    .. testoutput::

        {'encoder_out': 2, 'state_out': 4}

    """

    framework = "torch"

    def __init__(self, config):
        self.config = config

    @abc.abstractmethod
    def forward(self, input_dict: dict, **kwargs) -> dict:
        """Returns the latent of the encoder for the given inputs.

        This method is called by the forwarding method of the respective framework
        that is itself wrapped by RLlib in order to check model inputs and outputs.

        The input dict contains at minimum the observation and the state of the encoder
        (None for stateless encoders).
        The output dict contains at minimum the latent and the state of the encoder
        (None for stateless encoders).
        To establish an agreement between the encoder and RLModules, these values
        have the fixed keys `Columns.OBS` for the `input_dict`,
        and `ACTOR` and `CRITIC` for the returned dict.

        Args:
            input_dict: The input tensors. Must contain at a minimum the keys
                Columns.OBS and Columns.STATE_IN (which might be None for stateless
                encoders).
            **kwargs: Forward compatibility kwargs.

        Returns:
            The output tensors. Must contain at a minimum the key ENCODER_OUT.
        """

    def __call__(self, *args, **kwargs):
        return self.forward(*args, **kwargs)


@ExperimentalAPI
class ActorCriticEncoder(Encoder):
    """An encoder that potentially holds two stateless encoders.

    This is a special case of Encoder that can either enclose a single,
    shared encoder or two separate encoders: One for the actor and one for the
    critic. The two encoders are of the same type, and we can therefore make the
    assumption that they have the same input and output specs.
    """

    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)

        if config.shared:
            self.encoder = config.base_encoder_config.build(framework=self.framework)
        else:
            self.actor_encoder = config.base_encoder_config.build(
                framework=self.framework
            )
            self.critic_encoder = None
            if not config.inference_only:
                self.critic_encoder = config.base_encoder_config.build(
                    framework=self.framework
                )

    @override(Encoder)
    def forward(self, inputs: dict, **kwargs) -> dict:
        if self.config.shared:
            encoder_outs = self.encoder(inputs, **kwargs)
            return {
                ENCODER_OUT: {
                    ACTOR: encoder_outs[ENCODER_OUT],
                    **(
                        {}
                        if self.config.inference_only
                        else {CRITIC: encoder_outs[ENCODER_OUT]}
                    ),
                }
            }
        else:
            # Encoders should not modify inputs, so we can pass the same inputs
            actor_out = self.actor_encoder(inputs, **kwargs)
            if self.critic_encoder:
                critic_out = self.critic_encoder(inputs, **kwargs)

            return {
                ENCODER_OUT: {
                    ACTOR: actor_out[ENCODER_OUT],
                    **(
                        {}
                        if self.config.inference_only
                        else {CRITIC: critic_out[ENCODER_OUT]}
                    ),
                }
            }


@ExperimentalAPI
class StatefulActorCriticEncoder(Encoder):
    """An encoder that potentially holds two potentially stateful encoders.

    This is a special case of Encoder that can either enclose a single,
    shared encoder or two separate encoders: One for the actor and one for the
    critic. The two encoders are of the same type, and we can therefore make the
    assumption that they have the same input and output specs.

    If this encoder wraps a single encoder, state in input- and output dicts
    is simply stored under the key `STATE_IN` and `STATE_OUT`, respectively.
    If this encoder wraps two encoders, state in input- and output dicts is
    stored under the keys `(STATE_IN, ACTOR)` and `(STATE_IN, CRITIC)` and
    `(STATE_OUT, ACTOR)` and `(STATE_OUT, CRITIC)`, respectively.
    """

    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)

        if config.shared:
            self.encoder = config.base_encoder_config.build(framework=self.framework)
        else:
            self.actor_encoder = config.base_encoder_config.build(
                framework=self.framework
            )
            self.critic_encoder = config.base_encoder_config.build(
                framework=self.framework
            )

    def get_initial_state(self):
        if self.config.shared:
            return self.encoder.get_initial_state()
        else:
            return {
                ACTOR: self.actor_encoder.get_initial_state(),
                CRITIC: self.critic_encoder.get_initial_state(),
            }

    @override(Encoder)
    def forward(self, inputs: dict, **kwargs) -> dict:
        outputs = {}

        if self.config.shared:
            outs = self.encoder(inputs, **kwargs)
            encoder_out = outs.pop(ENCODER_OUT)
            outputs[ENCODER_OUT] = {ACTOR: encoder_out, CRITIC: encoder_out}
            outputs[Columns.STATE_OUT] = outs[Columns.STATE_OUT]
        else:
            # Shallow copy inputs so that we can add states without modifying
            # original dict.
            actor_inputs = inputs.copy()
            critic_inputs = inputs.copy()
            actor_inputs[Columns.STATE_IN] = inputs[Columns.STATE_IN][ACTOR]
            critic_inputs[Columns.STATE_IN] = inputs[Columns.STATE_IN][CRITIC]

            actor_out = self.actor_encoder(actor_inputs, **kwargs)
            critic_out = self.critic_encoder(critic_inputs, **kwargs)

            outputs[ENCODER_OUT] = {
                ACTOR: actor_out[ENCODER_OUT],
                CRITIC: critic_out[ENCODER_OUT],
            }

            outputs[Columns.STATE_OUT] = {
                ACTOR: actor_out[Columns.STATE_OUT],
                CRITIC: critic_out[Columns.STATE_OUT],
            }

        return outputs


@DeveloperAPI
def tokenize(tokenizer: Encoder, inputs: dict, framework: str) -> dict:
    """Tokenizes the observations from the input dict.

    Args:
        tokenizer: The tokenizer to use.
        inputs: The input dict.

    Returns:
        The output dict.
    """
    # Tokenizer may depend solely on observations.
    obs = inputs[Columns.OBS]
    tokenizer_inputs = {Columns.OBS: obs}
    size = list(obs.size() if framework == "torch" else obs.shape)
    b_dim, t_dim = size[:2]
    fold, unfold = get_fold_unfold_fns(b_dim, t_dim, framework=framework)
    # Push through the tokenizer encoder.
    out = tokenizer(fold(tokenizer_inputs))
    out = out[ENCODER_OUT]
    # Then unfold batch- and time-dimensions again.
    return unfold(out)
