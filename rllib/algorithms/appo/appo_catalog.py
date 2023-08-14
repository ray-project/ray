from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog


class APPOCatalog(PPOCatalog):
    """The Catalog class used to build models for APPO.

    PPOCatalog provides the following models:
        - ActorCriticEncoder: The encoder used to encode the observations.
        - Pi Head: The head used to compute the policy logits.
        - Value Function Head: The head used to compute the value function.

    The ActorCriticEncoder is a wrapper around Encoders to produce separate outputs
    for the policy and value function. See implementations of PPORLModule for
    more details.

    Any custom ActorCriticEncoder can be built by overriding the
    build_actor_critic_encoder() method. Alternatively, the ActorCriticEncoderConfig
    at PPOCatalog.actor_critic_encoder_config can be overridden to build a custom
    ActorCriticEncoder during RLModule runtime.

    Any custom head can be built by overriding the build_pi_head() and build_vf_head()
    methods. Alternatively, the PiHeadConfig and VfHeadConfig can be overridden to
    build custom heads during RLModule runtime.
    """
