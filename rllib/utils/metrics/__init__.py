# Counters for sampling and training steps (env- and agent steps).
NUM_ENV_STEPS_SAMPLED = "num_env_steps_sampled"
NUM_AGENT_STEPS_SAMPLED = "num_agent_steps_sampled"
NUM_ENV_STEPS_TRAINED = "num_env_steps_trained"
NUM_AGENT_STEPS_TRAINED = "num_agent_steps_trained"

# Counters to track target network updates.
LAST_TARGET_UPDATE_TS = "last_target_update_ts"
NUM_TARGET_UPDATES = "num_target_updates"

# Performance timers (keys for metrics.timers).
APPLY_GRADS_TIMER = "apply_grad"
COMPUTE_GRADS_TIMER = "compute_grads"
WORKER_UPDATE_TIMER = "update"
GRAD_WAIT_TIMER = "grad_wait"
SAMPLE_TIMER = "sample"
LEARN_ON_BATCH_TIMER = "learn"
LOAD_BATCH_TIMER = "load"
