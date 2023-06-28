# Counters for sampling and training steps (env- and agent steps).
NUM_ENV_STEPS_SAMPLED = "num_env_steps_sampled"
NUM_AGENT_STEPS_SAMPLED = "num_agent_steps_sampled"
NUM_ENV_STEPS_SAMPLED_THIS_ITER = "num_env_steps_sampled_this_iter"
NUM_AGENT_STEPS_SAMPLED_THIS_ITER = "num_agent_steps_sampled_this_iter"
NUM_ENV_STEPS_TRAINED = "num_env_steps_trained"
NUM_AGENT_STEPS_TRAINED = "num_agent_steps_trained"
NUM_ENV_STEPS_TRAINED_THIS_ITER = "num_env_steps_trained_this_iter"
NUM_AGENT_STEPS_TRAINED_THIS_ITER = "num_agent_steps_trained_this_iter"

# Counters for keeping track of worker weight updates (synchronization
# between local worker and remote workers).
NUM_SYNCH_WORKER_WEIGHTS = "num_weight_broadcasts"
NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS = (
    "num_training_step_calls_since_last_synch_worker_weights"
)
# Number of total gradient updates that have been performed on a policy.
NUM_GRAD_UPDATES_LIFETIME = "num_grad_updates_lifetime"
# Average difference between the number of grad-updates that the policy/ies had
# that collected the training batch vs the policy that was just updated (trained).
# Good measuere for the off-policy'ness of training. Should be 0.0 for PPO and PG,
# small for IMPALA and APPO, and any (larger) value for DQN and other off-policy algos.
DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY = "diff_num_grad_updates_vs_sampler_policy"

# Counters to track target network updates.
LAST_TARGET_UPDATE_TS = "last_target_update_ts"
NUM_TARGET_UPDATES = "num_target_updates"

# Performance timers (keys for Algorithm._timers).
TRAINING_ITERATION_TIMER = "training_iteration"
APPLY_GRADS_TIMER = "apply_grad"
COMPUTE_GRADS_TIMER = "compute_grads"
GARBAGE_COLLECTION_TIMER = "garbage_collection"
SYNCH_WORKER_WEIGHTS_TIMER = "synch_weights"
GRAD_WAIT_TIMER = "grad_wait"
SAMPLE_TIMER = "sample"
LEARN_ON_BATCH_TIMER = "learn"
LOAD_BATCH_TIMER = "load"
TARGET_NET_UPDATE_TIMER = "target_net_update"

# learner
LEARNER_STATS_KEY = "learner_stats"
ALL_MODULES = "__all__"
