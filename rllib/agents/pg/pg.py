from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.utils.experimental_dsl import (
    ParallelRollouts, ConcatBatches, TrainOneStep, StandardMetricsReporting)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # No remote workers by default.
    "num_workers": 0,
    # Learning rate.
    "lr": 0.0004,
})
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config):
    if config["use_pytorch"]:
        from ray.rllib.agents.pg.pg_torch_policy import PGTorchPolicy
        return PGTorchPolicy
    else:
        return PGTFPolicy


# Experimental pipeline-based impl; enable with "use_pipeline_impl": True.
def training_pipeline(workers, config):
    # Collects experiences in parallel from multiple RolloutWorker actors.
    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    # Combine experiences batches until we hit `train_batch_size` in size.
    # Then, train the policy on those experiences and update the workers.
    train_op = rollouts \
        .combine(ConcatBatches(
            min_batch_size=config["train_batch_size"])) \
        .for_each(TrainOneStep(workers))

    # Add on the standard episode reward, etc. metrics reporting. This returns
    # a LocalIterator[metrics_dict] representing metrics for each train step.
    return StandardMetricsReporting(train_op, workers, config)


PGTrainer = build_trainer(
    name="PG",
    default_config=DEFAULT_CONFIG,
    default_policy=PGTFPolicy,
    get_policy_class=get_policy_class,
    training_pipeline=training_pipeline)
