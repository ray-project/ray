"""Deep Q learning graph

The functions in this file can are used to create the following functions:

======= act ========

  Function to chose an action given an observation

  Parameters
  ----------
  observation: object
    Observation that can be feed into the output of make_obs_ph
  stochastic: bool
    if set to False all the actions are always deterministic
    (default False)
  update_eps_ph: float
    update epsilon a new value, if negative not update happens
    (default: no update)

  Returns
  -------
  Tensor of dtype tf.int64 and shape (BATCH_SIZE,) with an action to be
  performed for every element of the batch.


======= train =======

  Function that takes a transition (s,a,r,s') and optimizes Bellman
  equation's error:

    td_error = Q(s,a) - (r + gamma * max_a' Q(s', a'))
    loss = huber_loss[td_error]

  Parameters
  ----------
  obs_t: object
    a batch of observations
  action: np.array
    actions that were selected upon seeing obs_t.
    dtype must be int32 and shape must be (batch_size,)
  reward: np.array
    immediate reward attained after executing those actions
    dtype must be float32 and shape must be (batch_size,)
  obs_tp1: object
    observations that followed obs_t
  done: np.array
    1 if obs_t was the last observation in the episode and 0 otherwise
    obs_tp1 gets ignored, but must be of the valid shape.
    dtype must be float32 and shape must be (batch_size,)
  weight: np.array
    imporance weights for every element of the batch (gradient is
    multiplied by the importance weight) dtype must be float32 and shape
    must be (batch_size,)

  Returns
  -------
  td_error: np.array
    a list of differences between Q(s,a) and the target in Bellman's
    equation. dtype is float32 and shape is (batch_size,)

======= update_target ========

  copy the parameters from optimized Q function to the target Q function.
  In Q learning we actually optimize the following error:

    Q(s,a) - (r + gamma * max_a' Q'(s', a'))

  Where Q' is lagging behind Q to stablize the learning. For example for
  Atari

  Q' is set to Q once every 10000 updates training steps.

"""
import tensorflow as tf
from ray.rllib.dqn.common import tf_util as U


def build_act(make_obs_ph, q_func, num_actions, scope="deepq", reuse=None):
  """Creates the act function:

  Parameters
  ----------
  make_obs_ph: str -> tf.placeholder or TfInput
    a function that take a name and creates a placeholder of input with
    that name
  q_func: (tf.Variable, int, str, bool) -> tf.Variable
    the model that takes the following inputs:
      observation_in: object
        the output of observation placeholder
      num_actions: int
        number of actions
      scope: str
      reuse: bool
        should be passed to outer variable scope
    and returns a tensor of shape (batch_size, num_actions) with values of
    every action.
  num_actions: int
    number of actions.
  scope: str or VariableScope
    optional scope for variable_scope.
  reuse: bool or None
    whether or not the variables should be reused. To be able to reuse the
    scope must be given.

  Returns
  -------
  act: (tf.Variable, bool, float) -> tf.Variable
    function to select and action given observation.
`   See the top of the file for details.
  """
  with tf.variable_scope(scope, reuse=reuse):
    observations_ph = U.ensure_tf_input(make_obs_ph("observation"))
    stochastic_ph = tf.placeholder(tf.bool, (), name="stochastic")
    update_eps_ph = tf.placeholder(tf.float32, (), name="update_eps")

    eps = tf.get_variable(
        "eps", (), initializer=tf.constant_initializer(0))

    q_values = q_func(observations_ph.get(), num_actions, scope="q_func")
    deterministic_actions = tf.argmax(q_values, axis=1)

    batch_size = tf.shape(observations_ph.get())[0]
    random_actions = tf.random_uniform(
        tf.stack([batch_size]), minval=0, maxval=num_actions, dtype=tf.int64)
    chose_random = tf.random_uniform(
        tf.stack([batch_size]), minval=0, maxval=1, dtype=tf.float32) < eps
    stochastic_actions = tf.where(
        chose_random, random_actions, deterministic_actions)

    output_actions = tf.cond(
        stochastic_ph, lambda: stochastic_actions,
        lambda: deterministic_actions)
    update_eps_expr = eps.assign(
        tf.cond(update_eps_ph >= 0, lambda: update_eps_ph, lambda: eps))

    act = U.function(
        inputs=[observations_ph, stochastic_ph, update_eps_ph],
        outputs=output_actions,
        givens={update_eps_ph: -1.0, stochastic_ph: True},
        updates=[update_eps_expr])
    return act


def build_train(
    make_obs_ph, q_func, num_actions, optimizer, grad_norm_clipping=None,
        gamma=1.0, double_q=True, scope="deepq", reuse=None):
  """Creates the train function:

  Parameters
  ----------
  make_obs_ph: str -> tf.placeholder or TfInput
    a function that takes a name and creates a placeholder of input with
    that name
  q_func: (tf.Variable, int, str, bool) -> tf.Variable
    the model that takes the following inputs:
      observation_in: object
        the output of observation placeholder
      num_actions: int
        number of actions
      scope: str
      reuse: bool
        should be passed to outer variable scope
    and returns a tensor of shape (batch_size, num_actions) with values of
    every action.
  num_actions: int
    number of actions
  reuse: bool
    whether or not to reuse the graph variables
  optimizer: tf.train.Optimizer
    optimizer to use for the Q-learning objective.
  grad_norm_clipping: float or None
    clip gradient norms to this value. If None no clipping is performed.
  gamma: float
    discount rate.
  double_q: bool
    if true will use Double Q Learning (https://arxiv.org/abs/1509.06461).
    In general it is a good idea to keep it enabled.
  scope: str or VariableScope
    optional scope for variable_scope.
  reuse: bool or None
    whether or not the variables should be reused. To be able to reuse the
    scope must be given.

  Returns
  -------
  act: (tf.Variable, bool, float) -> tf.Variable
    function to select and action given observation.
`   See the top of the file for details.
  train: (object, np.array, np.array, object, np.array, np.array) -> np.array
    optimize the error in Bellman's equation.
`   See the top of the file for details.
  update_target: () -> ()
    copy the parameters from optimized Q function to the target Q function.
`   See the top of the file for details.
  debug: {str: function}
    a bunch of functions to print debug data like q_values.
  """
  act_f = build_act(make_obs_ph, q_func, num_actions, scope=scope, reuse=reuse)

  with tf.variable_scope(scope, reuse=reuse):
    # set up placeholders
    obs_t_input = U.ensure_tf_input(make_obs_ph("obs_t"))
    act_t_ph = tf.placeholder(tf.int32, [None], name="action")
    rew_t_ph = tf.placeholder(tf.float32, [None], name="reward")
    obs_tp1_input = U.ensure_tf_input(make_obs_ph("obs_tp1"))
    done_mask_ph = tf.placeholder(tf.float32, [None], name="done")
    importance_weights_ph = tf.placeholder(tf.float32, [None], name="weight")

    # q network evaluation
    q_t = q_func(
        obs_t_input.get(), num_actions, scope="q_func",
        reuse=True)  # reuse parameters from act
    q_func_vars = U.scope_vars(U.absolute_scope_name("q_func"))

    # target q network evalution
    q_tp1 = q_func(obs_tp1_input.get(), num_actions, scope="target_q_func")
    target_q_func_vars = U.scope_vars(U.absolute_scope_name("target_q_func"))

    # q scores for actions which we know were selected in the given state.
    q_t_selected = tf.reduce_sum(q_t * tf.one_hot(act_t_ph, num_actions), 1)

    # compute estimate of best possible value starting from state at t + 1
    if double_q:
      q_tp1_using_online_net = q_func(
          obs_tp1_input.get(), num_actions, scope="q_func", reuse=True)
      q_tp1_best_using_online_net = tf.arg_max(q_tp1_using_online_net, 1)
      q_tp1_best = tf.reduce_sum(
          q_tp1 * tf.one_hot(q_tp1_best_using_online_net, num_actions), 1)
    else:
      q_tp1_best = tf.reduce_max(q_tp1, 1)
    q_tp1_best_masked = (1.0 - done_mask_ph) * q_tp1_best

    # compute RHS of bellman equation
    q_t_selected_target = rew_t_ph + gamma * q_tp1_best_masked

    # compute the error (potentially clipped)
    td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
    errors = U.huber_loss(td_error)
    weighted_error = tf.reduce_mean(importance_weights_ph * errors)
    # compute optimization op (potentially with gradient clipping)
    if grad_norm_clipping is not None:
      optimize_expr = U.minimize_and_clip(
          optimizer, weighted_error, var_list=q_func_vars,
          clip_val=grad_norm_clipping)
    else:
      optimize_expr = optimizer.minimize(weighted_error, var_list=q_func_vars)

    # update_target_fn will be called periodically to copy Q network to
    # target Q network
    update_target_expr = []
    for var, var_target in zip(
        sorted(q_func_vars, key=lambda v: v.name),
            sorted(target_q_func_vars, key=lambda v: v.name)):
      update_target_expr.append(var_target.assign(var))
    update_target_expr = tf.group(*update_target_expr)

    # Create callable functions
    train = U.function(
        inputs=[
            obs_t_input,
            act_t_ph,
            rew_t_ph,
            obs_tp1_input,
            done_mask_ph,
            importance_weights_ph
        ],
        outputs=td_error,
        updates=[optimize_expr])
    update_target = U.function([], [], updates=[update_target_expr])

    q_values = U.function([obs_t_input], q_t)

    return act_f, train, update_target, {'q_values': q_values}
