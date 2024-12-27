import torch

from ray.rllib.core import Columns
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule


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
        # Embeddings can be found in the batch under the "encoder_embeddings" key.
        embeddings = batch["encoder_embeddings"]
        logits = self._pi_head(embeddings)
        return {Columns.ACTION_DIST_INPUTS: logits}


# __sphinx_doc_policy_2_end__


# __sphinx_doc_mrlm_begin__
class VPGMultiRLModuleWithSharedEncoder(MultiRLModule):
    """VPG (vanilla pol. gradient)-style MultiRLModule handling a shared encoder.
    # __sphinx_doc_mrlm_end__

        This MultiRLModule needs to be configured appropriately as follows:

        .. testcode::

    # __sphinx_doc_how_to_run_begin__
            import gymnasium as gym
            from ray.rllib.algorithms.ppo import PPOConfig
            from ray.rllib.core import MultiRLModuleSpec, RLModuleSpec
            from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole

            single_agent_env = gym.make("CartPole-v1")

            EMBEDDING_DIM = 64  # encoder output dim

            config = (
                PPOConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .multi_agent(
                    # Declare the two policies trained.
                    policies={"p0", "p1"},
                    # Agent IDs of `MultiAgentCartPole` are 0 and 1. They are mapped to
                    # the two policies with ModuleIDs "p0" and "p1", respectively.
                    policy_mapping_fn=lambda agent_id, episode, **kw: f"p{agent_id}"
                )
                .rl_module(
                    rl_module_spec=MultiRLModuleSpec(
                        rl_module_specs={
                            # Shared encoder.
                            SHARED_ENCODER_ID: RLModuleSpec(
                                module_class=SharedEncoder,
                                model_config={"embedding_dim": EMBEDDING_DIM},
                                observation_space=single_agent_env.observation_space,
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
            algo = config.build()
            print(algo.get_module())
    # __sphinx_doc_how_to_run_end__

        Also note that in order to learn properly, a special, multi-agent Learner
        accounting for the shared encoder must be setup. This Learner should have only
        one optimizer (used to train all submodules: encoder and the n policy nets) in
        order to not destabilize learning. The latter would happen if more than one
        optimizer would try to alternatingly optimize the same shared encoder submodule.
    # __sphinx_doc_mrlm_2_begin__
    """

    def setup(self):
        # Call the super's setup().
        super().setup()

        # Assert, we have the shared encoder submodule.
        assert (
            SHARED_ENCODER_ID in self._rl_modules
            and isinstance(self._rl_modules[SHARED_ENCODER_ID], SharedEncoder)
            and len(self._rl_modules) > 1
        )
        # Assign the encoder to a convenience attribute.
        self.encoder = self._rl_modules[SHARED_ENCODER_ID]

    def _forward(self, batch, **kwargs):
        # Collect our policies' outputs in this dict.
        outputs = {}

        # Loop through the policy nets (through the given batch's keys).
        for policy_id, policy_batch in batch.items():
            rl_module = self._rl_modules[policy_id]

            # Pass policy's observations through shared encoder to get the features for
            # this policy.
            policy_batch["encoder_embeddings"] = self.encoder._forward(batch[policy_id])

            # Pass the policy's embeddings through the policy net.
            outputs[policy_id] = rl_module._forward(batch[policy_id], **kwargs)

        return outputs


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
        return {"encoder_embeddings": self._net(batch[Columns.OBS])}


# __sphinx_doc_encoder_end__
