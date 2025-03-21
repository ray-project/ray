from ray.rllib.core import ALL_MODULES  # noqa


# Algorithm ResultDict keys.
AGGREGATOR_ACTOR_RESULTS = "aggregator_actors"
EVALUATION_RESULTS = "evaluation"
ENV_RUNNER_RESULTS = "env_runners"
REPLAY_BUFFER_RESULTS = "replay_buffer"
LEARNER_GROUP = "learner_group"
LEARNER_RESULTS = "learners"
FAULT_TOLERANCE_STATS = "fault_tolerance"
TIMERS = "timers"

# RLModule metrics.
NUM_TRAINABLE_PARAMETERS = "num_trainable_parameters"
NUM_NON_TRAINABLE_PARAMETERS = "num_non_trainable_parameters"

# Number of times `training_step()` was called in one iteration.
NUM_TRAINING_STEP_CALLS_PER_ITERATION = "num_training_step_calls_per_iteration"

# Counters for sampling, sampling (on eval workers) and
# training steps (env- and agent steps).
MEAN_NUM_EPISODE_LISTS_RECEIVED = "mean_num_episode_lists_received"
NUM_AGENT_STEPS_SAMPLED = "num_agent_steps_sampled"
NUM_AGENT_STEPS_SAMPLED_LIFETIME = "num_agent_steps_sampled_lifetime"
NUM_AGENT_STEPS_SAMPLED_THIS_ITER = "num_agent_steps_sampled_this_iter"  # @OldAPIStack
NUM_ENV_STEPS_SAMPLED = "num_env_steps_sampled"
NUM_ENV_STEPS_SAMPLED_LIFETIME = "num_env_steps_sampled_lifetime"
NUM_ENV_STEPS_SAMPLED_PER_SECOND = "num_env_steps_sampled_per_second"
NUM_ENV_STEPS_SAMPLED_THIS_ITER = "num_env_steps_sampled_this_iter"  # @OldAPIStack
NUM_ENV_STEPS_SAMPLED_FOR_EVALUATION_THIS_ITER = (
    "num_env_steps_sampled_for_evaluation_this_iter"
)
NUM_MODULE_STEPS_SAMPLED = "num_module_steps_sampled"
NUM_MODULE_STEPS_SAMPLED_LIFETIME = "num_module_steps_sampled_lifetime"
ENV_TO_MODULE_SUM_EPISODES_LENGTH_IN = "env_to_module_sum_episodes_length_in"
ENV_TO_MODULE_SUM_EPISODES_LENGTH_OUT = "env_to_module_sum_episodes_length_out"

# Counters for adding and evicting in replay buffers.
ACTUAL_N_STEP = "actual_n_step"
AGENT_ACTUAL_N_STEP = "agent_actual_n_step"
AGENT_STEP_UTILIZATION = "agent_step_utilization"
MODULE_ACTUAL_N_STEP = "module_actual_n_step"
MODULE_STEP_UTILIZATION = "module_step_utilization"
ENV_STEP_UTILIZATION = "env_step_utilization"
NUM_AGENT_EPISODES_STORED = "num_agent_episodes"
NUM_AGENT_EPISODES_ADDED = "num_agent_episodes_added"
NUM_AGENT_EPISODES_ADDED_LIFETIME = "num_agent_episodes_added_lifetime"
NUM_AGENT_EPISODES_EVICTED = "num_agent_episodes_evicted"
NUM_AGENT_EPISODES_EVICTED_LIFETIME = "num_agent_episodes_evicted_lifetime"
NUM_AGENT_EPISODES_PER_SAMPLE = "num_agent_episodes_per_sample"
NUM_AGENT_RESAMPLES = "num_agent_resamples"
NUM_AGENT_STEPS_ADDED = "num_agent_steps_added"
NUM_AGENT_STEPS_ADDED_LIFETIME = "num_agent_steps_added_lifetime"
NUM_AGENT_STEPS_EVICTED = "num_agent_steps_evicted"
NUM_AGENT_STEPS_EVICTED_LIFETIME = "num_agent_steps_evicted_lifetime"
NUM_AGENT_STEPS_PER_SAMPLE = "num_agent_steps_per_sample"
NUM_AGENT_STEPS_PER_SAMPLE_LIFETIME = "num_agent_steps_per_sample_lifetime"
NUM_AGENT_STEPS_STORED = "num_agent_steps_stored"
NUM_ENV_STEPS_ADDED = "num_env_steps_added"
NUM_ENV_STEPS_ADDED_LIFETIME = "num_env_steps_added_lifetime"
NUM_ENV_STEPS_EVICTED = "num_env_steps_evicted"
NUM_ENV_STEPS_EVICTED_LIFETIME = "num_env_steps_evicted_lifetime"
NUM_ENV_STEPS_PER_SAMPLE = "num_env_steps_per_sample"
NUM_ENV_STEPS_PER_SAMPLE_LIFETIME = "num_env_steps_per_sample_lifetime"
NUM_ENV_STEPS_STORED = "num_env_steps_stored"
NUM_EPISODES_STORED = "num_episodes_stored"
NUM_EPISODES_ADDED = "num_episodes_added"
NUM_EPISODES_ADDED_LIFETIME = "num_episodes_added_lifetime"
NUM_EPISODES_EVICTED = "num_episodes_evicted"
NUM_EPISODES_EVICTED_LIFETIME = "num_episodes_evicted_lifetime"
NUM_EPISODES_PER_SAMPLE = "num_episodes_per_sample"
NUM_RESAMPLES = "num_resamples"
NUM_MODULE_EPISODES_STORED = "num_module_episodes"
NUM_MODULE_EPISODES_ADDED = "num_module_episodes_added"
NUM_MODULE_EPISODES_ADDED_LIFETIME = "num_module_episodes_added_lifetime"
NUM_MODULE_EPISODES_EVICTED = "num_module_episodes_evicted"
NUM_MODULE_EPISODES_EVICTED_LIFETIME = "num_module_episodes_evicted_lifetime"
NUM_MODULE_EPISODES_PER_SAMPLE = "num_module_episodes_per_sample"
NUM_MODULE_RESAMPLES = "num_module_resamples"
NUM_MODULE_STEPS_ADDED = "num_module_steps_added"
NUM_MODULE_STEPS_ADDED_LIFETIME = "num_module_steps_added_lifetime"
NUM_MODULE_STEPS_EVICTED = "num_module_steps_evicted"
NUM_MODULE_STEPS_EVICTED_LIFETIME = "num_module_steps_evicted_lifetime"
NUM_MODULE_STEPS_PER_SAMPLE = "num_module_steps_per_sample"
NUM_MODULE_STEPS_PER_SAMPLE_LIFETIME = "num_module_steps_per_sample_lifetime"
NUM_MODULE_STEPS_STORED = "num_module_steps_stored"

EPISODE_DURATION_SEC_MEAN = "episode_duration_sec_mean"
EPISODE_LEN_MEAN = "episode_len_mean"
EPISODE_LEN_MAX = "episode_len_max"
EPISODE_LEN_MIN = "episode_len_min"
EPISODE_RETURN_MEAN = "episode_return_mean"
EPISODE_RETURN_MAX = "episode_return_max"
EPISODE_RETURN_MIN = "episode_return_min"
NUM_EPISODES = "num_episodes"
NUM_EPISODES_LIFETIME = "num_episodes_lifetime"
TIME_BETWEEN_SAMPLING = "time_between_sampling"

DATASET_NUM_ITERS_TRAINED = "dataset_num_iters_trained"
DATASET_NUM_ITERS_TRAINED_LIFETIME = "dataset_num_iters_trained_lifetime"
MEAN_NUM_LEARNER_GROUP_UPDATE_CALLED = "mean_num_learner_group_update_called"
MEAN_NUM_LEARNER_RESULTS_RECEIVED = "mean_num_learner_results_received"
NUM_AGENT_STEPS_TRAINED = "num_agent_steps_trained"
NUM_AGENT_STEPS_TRAINED_LIFETIME = "num_agent_steps_trained_lifetime"
NUM_AGENT_STEPS_TRAINED_THIS_ITER = "num_agent_steps_trained_this_iter"  # @OldAPIStack
NUM_ENV_STEPS_TRAINED = "num_env_steps_trained"
NUM_ENV_STEPS_TRAINED_LIFETIME = "num_env_steps_trained_lifetime"
NUM_ENV_STEPS_TRAINED_THIS_ITER = "num_env_steps_trained_this_iter"  # @OldAPIStack
NUM_MODULE_STEPS_TRAINED = "num_module_steps_trained"
NUM_MODULE_STEPS_TRAINED_LIFETIME = "num_module_steps_trained_lifetime"
MODULE_TRAIN_BATCH_SIZE_MEAN = "module_train_batch_size_mean"
LEARNER_CONNECTOR_SUM_EPISODES_LENGTH_IN = "learner_connector_sum_episodes_length_in"
LEARNER_CONNECTOR_SUM_EPISODES_LENGTH_OUT = "learner_connector_sum_episodes_length_out"

# Backward compatibility: Replace with num_env_steps_... or num_agent_steps_...
STEPS_TRAINED_THIS_ITER_COUNTER = "num_steps_trained_this_iter"

# Counters for keeping track of worker weight updates (synchronization
# between local worker and remote workers).
NUM_SYNCH_WORKER_WEIGHTS = "num_weight_broadcasts"
NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS = (
    "num_training_step_calls_since_last_synch_worker_weights"
)
# The running sequence number for a set of NN weights. If a worker's NN has a
# lower sequence number than some weights coming in for an update, the worker
# should perform the update, otherwise ignore the incoming weights (they are older
# or the same) as/than the ones it already has.
WEIGHTS_SEQ_NO = "weights_seq_no"
# Number of total gradient updates that have been performed on a policy.
NUM_GRAD_UPDATES_LIFETIME = "num_grad_updates_lifetime"
# Average difference between the number of grad-updates that the policy/ies had
# that collected the training batch vs the policy that was just updated (trained).
# Good measure for the off-policy'ness of training. Should be 0.0 for PPO and PG,
# small for IMPALA and APPO, and any (larger) value for DQN and other off-policy algos.
DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY = "diff_num_grad_updates_vs_sampler_policy"

# Counters to track target network updates.
LAST_TARGET_UPDATE_TS = "last_target_update_ts"
NUM_TARGET_UPDATES = "num_target_updates"

# Performance timers
# ------------------
# Duration of n `Algorithm.training_step()` calls making up one "iteration".
# Note that n may be >1 if the user has set up a min time (sec) or timesteps per
# iteration.
TRAINING_ITERATION_TIMER = "training_iteration"
# Duration of a `Algorithm.evaluate()` call.
EVALUATION_ITERATION_TIMER = "evaluation_iteration"
# Duration of a single `training_step()` call.
TRAINING_STEP_TIMER = "training_step"
APPLY_GRADS_TIMER = "apply_grad"
COMPUTE_GRADS_TIMER = "compute_grads"
GARBAGE_COLLECTION_TIMER = "garbage_collection"
RESTORE_ENV_RUNNERS_TIMER = "restore_env_runners"
RESTORE_EVAL_ENV_RUNNERS_TIMER = "restore_eval_env_runners"
SYNCH_WORKER_WEIGHTS_TIMER = "synch_weights"
SYNCH_ENV_CONNECTOR_STATES_TIMER = "synch_env_connectors"
SYNCH_EVAL_ENV_CONNECTOR_STATES_TIMER = "synch_eval_env_connectors"
GRAD_WAIT_TIMER = "grad_wait"
SAMPLE_TIMER = "sample"  # @OldAPIStack
ENV_RUNNER_SAMPLING_TIMER = "env_runner_sampling_timer"
ENV_RESET_TIMER = "env_reset_timer"
ENV_STEP_TIMER = "env_step_timer"
ENV_TO_MODULE_CONNECTOR = "env_to_module_connector"
RLMODULE_INFERENCE_TIMER = "rlmodule_inference_timer"
MODULE_TO_ENV_CONNECTOR = "module_to_env_connector"
OFFLINE_SAMPLING_TIMER = "offline_sampling_timer"
REPLAY_BUFFER_ADD_DATA_TIMER = "replay_buffer_add_data_timer"
REPLAY_BUFFER_SAMPLE_TIMER = "replay_buffer_sampling_timer"
REPLAY_BUFFER_UPDATE_PRIOS_TIMER = "replay_buffer_update_prios_timer"
LEARNER_CONNECTOR = "learner_connector"
LEARNER_UPDATE_TIMER = "learner_update_timer"
LEARN_ON_BATCH_TIMER = "learn"  # @OldAPIStack
LOAD_BATCH_TIMER = "load"
TARGET_NET_UPDATE_TIMER = "target_net_update"
CONNECTOR_PIPELINE_TIMER = "connector_pipeline_timer"
CONNECTOR_TIMERS = "connectors"

# Learner.
LEARNER_STATS_KEY = "learner_stats"
TD_ERROR_KEY = "td_error"
