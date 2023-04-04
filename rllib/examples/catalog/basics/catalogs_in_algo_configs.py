"""
This files demonstrates how to specify a Catalog for an RLModule to use through
AlgorithmConfig.
"""
# __sphinx_doc_begin__
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec


class MyPPOCatalog(PPOCatalog):
    def __init__(self, *args, **kwargs):
        print("Hi from within PPORLModule!")
        super.__init__(*args, **kwargs)


config = (
    PPOConfig()
    .environment("CartPole-v1")
    .framework("torch")
    .rl_module(_enable_rl_module_api=True)
)

# Specify the catalog to use for the PPORLModule.
config = config.rl_module(rl_module_spec=SingleAgentRLModuleSpec(
    catalog_class=MyPPOCatalog))
# This is how RLlib constructs a PPORLModule
# It will say "Hi from within PPORLModule!".
ppo_rl_module = config.rl_module_spec.build()
# __sphinx_doc_end__
