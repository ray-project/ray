from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Columns:
    """Definitions of common column names for RL data, e.g. 'obs', 'rewards', etc..

    Note that this replaces the `SampleBatch` and `Postprocessing` columns (of the same
    name).
    """

    # Observation received from an environment after `reset()` or `step()`.
    OBS = "obs"
    # Infos received from an environment after `reset()` or `step()`.
    INFOS = "infos"

    # Action computed/sampled by an RLModule.
    ACTIONS = "actions"
    # Action actually sent to the (gymnasium) `Env.step()` method.
    ACTIONS_FOR_ENV = "actions_for_env"
    # Reward returned by `env.step()`.
    REWARDS = "rewards"
    # Termination signal received from an environment after `step()`.
    TERMINATEDS = "terminateds"
    # Truncation signal received from an environment after `step()` (e.g. because
    # of a reached time limit).
    TRUNCATEDS = "truncateds"

    # Next observation: Only used by algorithms that need to look at TD-data for
    # training, such as off-policy/DQN algos.
    NEXT_OBS = "new_obs"

    # Uniquely identifies an episode
    EPS_ID = "eps_id"
    AGENT_ID = "agent_id"
    MODULE_ID = "module_id"

    # The size of non-zero-padded data within a (e.g. LSTM) zero-padded
    # (B, T, ...)-style train batch.
    SEQ_LENS = "seq_lens"
    # Episode timestep counter.
    T = "t"

    # Common extra RLModule output keys.
    STATE_IN = "state_in"
    STATE_OUT = "state_out"
    EMBEDDINGS = "embeddings"
    ACTION_DIST_INPUTS = "action_dist_inputs"
    ACTION_PROB = "action_prob"
    ACTION_LOGP = "action_logp"

    # Value function predictions.
    VF_PREDS = "vf_preds"
    # Values, predicted at one timestep beyond the last timestep taken.
    # These are usually calculated via the value function network using the final
    # observation (and in case of an RNN: the last returned internal state).
    VALUES_BOOTSTRAPPED = "values_bootstrapped"

    # Postprocessing columns.
    ADVANTAGES = "advantages"
    VALUE_TARGETS = "value_targets"

    # Intrinsic rewards (learning with curiosity).
    INTRINSIC_REWARDS = "intrinsic_rewards"

    # Loss mask. If provided in a train batch, a Learner's compute_loss_for_module
    # method should respect the False-set value in here and mask out the respective
    # items form the loss.
    LOSS_MASK = "loss_mask"
