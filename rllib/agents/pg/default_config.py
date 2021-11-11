from ray.rllib.agents.trainer import with_common_config

# yapf: disable
# __sphinx_doc_begin__

# Add the following (PG-specific) updates to the (base) `Trainer` config in
# rllib/agents/trainer.py (`COMMON_CONFIG` dict).
DEFAULT_CONFIG = with_common_config({
    # No remote workers by default.
    "num_workers": 0,
    # Learning rate.
    "lr": 0.0004,
})

# __sphinx_doc_end__
# yapf: enable
