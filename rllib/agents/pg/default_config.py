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

    # Experimental: By default, switch off preprocessors for PG.
    "_disable_preprocessor_api": True,

    # PG is the first algo (experimental) to not use the distr. exec API
    # anymore.
    "_disable_execution_plan_api": True,
})

# __sphinx_doc_end__
# yapf: enable
