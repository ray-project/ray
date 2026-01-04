from typing import (
    Any,
    Dict,
    Union,
)

import torch

from ray.rllib.core import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import ModuleID

SHARED_ENCODER_ID = "shared_encoder"


# __sphinx_doc_policy_begin__
class VPGPolicyAfterSharedEncoder(TorchRLModule):
    """A VPG (vanilla pol. gradient)-style RLModule using a shared encoder.
    # __sphinx_doc_policy_end__

        The shared encoder RLModule must be held by the same MultiRLModule, under which
        this RLModule resides. The shared encoder's forward is called before this
        RLModule's forward and returns the embeddings under the "encoder_embeddings"
        key.
    # __sphinx_doc_policy_2_begin__
    """

    def setup(self):
        super().setup()

        # Incoming feature dim from the shared encoder.
        embedding_dim = self.model_config["embedding_dim"]
        hidden_dim = self.model_config["hidden_dim"]

        self._pi_head = torch.nn.Sequential(
            torch.nn.Linear(embedding_dim, hidden_dim),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim, self.action_space.n),
        )

    def _forward(self, batch, **kwargs):
        embeddings = batch[ENCODER_OUT]  # Get the output of the encoder
        logits = self._pi_head(embeddings)
        return {Columns.ACTION_DIST_INPUTS: logits}


# __sphinx_doc_policy_2_end__


# __sphinx_doc_mrlm_begin__
class VPGMultiRLModuleWithSharedEncoder(MultiRLModule):
    """VPG (vanilla pol. gradient)-style MultiRLModule handling a shared encoder.
    # __sphinx_doc_mrlm_end__

        This MultiRLModule needs to be configured appropriately as below.

        .. testcode::

            # __sphinx_doc_how_to_run_begin__
            import gymnasium as gym
            from ray.rllib.core.rl_module.rl_module import RLModuleSpec
            from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec

            from ray.rllib.examples.algorithms.classes.vpg import VPGConfig
            from ray.rllib.examples.learners.classes.vpg_torch_learner_shared_optimizer import VPGTorchLearnerSharedOptimizer
            from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
            from ray.rllib.examples.rl_modules.classes.vpg_using_shared_encoder_rlm import (
                SHARED_ENCODER_ID,
                SharedEncoder,
                VPGPolicyAfterSharedEncoder,
                VPGMultiRLModuleWithSharedEncoder,
            )

            single_agent_env = gym.make("CartPole-v1")

            EMBEDDING_DIM = 64  # encoder output dim

            config = (
                VPGConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .training(
                    learner_class=VPGTorchLearnerSharedOptimizer,
                )
                .multi_agent(
                    # Declare the two policies trained.
                    policies={"p0", "p1"},
                    # Agent IDs of `MultiAgentCartPole` are 0 and 1. They are mapped to
                    # the two policies with ModuleIDs "p0" and "p1", respectively.
                    policy_mapping_fn=lambda agent_id, episode, **kw: f"p{agent_id}"
                )
                .rl_module(
                    rl_module_spec=MultiRLModuleSpec(
                        multi_rl_module_class=VPGMultiRLModuleWithSharedEncoder,
                        rl_module_specs={
                            # Shared encoder.
                            SHARED_ENCODER_ID: RLModuleSpec(
                                module_class=SharedEncoder,
                                model_config={"embedding_dim": EMBEDDING_DIM},
                                observation_space=single_agent_env.observation_space,
                                action_space=single_agent_env.action_space,
                            ),
                            # Large policy net.
                            "p0": RLModuleSpec(
                                module_class=VPGPolicyAfterSharedEncoder,
                                model_config={
                                    "embedding_dim": EMBEDDING_DIM,
                                    "hidden_dim": 1024,
                                },
                            ),
                            # Small policy net.
                            "p1": RLModuleSpec(
                                module_class=VPGPolicyAfterSharedEncoder,
                                model_config={
                                    "embedding_dim": EMBEDDING_DIM,
                                    "hidden_dim": 64,
                                },
                            ),
                        },
                    ),
                )
            )
            algo = config.build_algo()
            print(algo.train())
            # __sphinx_doc_how_to_run_end__
    # __sphinx_doc_mrlm_2_begin__
    """

    def setup(self):
        # Call the super's setup().
        super().setup()
        # Assert, we have the shared encoder submodule.
        assert SHARED_ENCODER_ID in self._rl_modules and len(self._rl_modules) > 1
        # Assign the encoder to a convenience attribute.
        self.encoder = self._rl_modules[SHARED_ENCODER_ID]

    def _forward(self, batch, forward_type, **kwargs):
        # Collect our policies' outputs in this dict.
        fwd_out = {}
        # Loop through the policy nets (through the given batch's keys).
        for policy_id, policy_batch in batch.items():
            # Feed this policy's observation into the shared encoder
            encoder_output = self.encoder._forward(batch[policy_id])
            policy_batch[ENCODER_OUT] = encoder_output[ENCODER_OUT]
            # Get the desired module
            m = getattr(self._rl_modules[policy_id], forward_type)
            # Pass the policy's embeddings through the policy net.
            fwd_out[policy_id] = m(batch[policy_id], **kwargs)
        return fwd_out

    # These methods could probably stand to be adjusted in MultiRLModule using something like this, so that subclasses that tweak _forward don't need to rewrite all of them. The prior implementation errored out because of this issue.
    @override(MultiRLModule)
    def _forward_inference(
        self, batch: Dict[str, Any], **kwargs
    ) -> Union[Dict[str, Any], Dict[ModuleID, Dict[str, Any]]]:
        return self._forward(batch, "_forward_inference", **kwargs)

    @override(MultiRLModule)
    def _forward_exploration(
        self, batch: Dict[str, Any], **kwargs
    ) -> Union[Dict[str, Any], Dict[ModuleID, Dict[str, Any]]]:
        return self._forward(batch, "_forward_exploration", **kwargs)

    @override(MultiRLModule)
    def _forward_train(
        self, batch: Dict[str, Any], **kwargs
    ) -> Union[Dict[str, Any], Dict[ModuleID, Dict[str, Any]]]:
        return self._forward(batch, "_forward_train", **kwargs)


# __sphinx_doc_mrlm_2_end__


# __sphinx_doc_encoder_begin__
class SharedEncoder(TorchRLModule):
    """A shared encoder that can be used with `VPGMultiRLModuleWithSharedEncoder`."""

    def setup(self):
        super().setup()

        input_dim = self.observation_space.shape[0]
        embedding_dim = self.model_config["embedding_dim"]

        # A very simple encoder network.
        self._net = torch.nn.Sequential(
            torch.nn.Linear(input_dim, embedding_dim),
        )

    def _forward(self, batch, **kwargs):
        # Pass observations through the net and return outputs.
        return {ENCODER_OUT: self._net(batch[Columns.OBS])}


# __sphinx_doc_encoder_end__


# __sphinx_doc_ns_encoder_begin__
class VPGIndividualEncoder(torch.nn.Module):
    def __init__(self, observation_space, embedding_dim):
        """
        An individual version of SharedEncoder, supporting direct comparison between
        the two architectures.
        """
        super().__init__()

        input_dim = observation_space.shape[0]

        # A very simple encoder network.
        self._net = torch.nn.Sequential(
            torch.nn.Linear(input_dim, embedding_dim),
        )

    def forward(self, batch, **kwargs):
        # Pass observations through the net and return outputs.
        return {ENCODER_OUT: self._net(batch[Columns.OBS])}


# __sphinx_doc_ns_encoder_end__


# __sphinx_doc_ns_policy_begin__
class VPGPolicyNoSharedEncoder(TorchRLModule):
    """
    A VPG (vanilla pol. gradient)-style RLModule that doesn't use a shared encoder.
    Facilitates experiments comparing shared and individual encoder architectures.
    """

    def setup(self):
        super().setup()

        # Incoming feature dim from the encoder.
        embedding_dim = self.model_config["embedding_dim"]
        hidden_dim = self.model_config["hidden_dim"]

        self._pi_head = torch.nn.Sequential(
            torch.nn.Linear(embedding_dim, hidden_dim),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim, self.action_space.n),
        )
        self.encoder = VPGIndividualEncoder(self.observation_space, embedding_dim)

    def _forward(self, batch, **kwargs):
        if ENCODER_OUT not in batch:
            batch = self.encoder(batch)
        embeddings = batch[ENCODER_OUT]
        logits = self._pi_head(embeddings)
        return {Columns.ACTION_DIST_INPUTS: logits}


# __sphinx_doc_ns_policy_end__
