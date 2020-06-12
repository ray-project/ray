import logging
import numpy as np

from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf = try_import_tf()
if tf:
    eager_mode = None
    try:
        from tensorflow.python.eager.context import eager_mode
    except (ImportError, ModuleNotFoundError):
        pass

torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


def framework_iterator(config=None,
                       frameworks=("tf", "tfe", "torch"),
                       session=False):
    """An generator that allows for looping through n frameworks for testing.

    Provides the correct config entries ("framework") as well
    as the correct eager/non-eager contexts for tfe/tf.

    Args:
        config (Optional[dict]): An optional config dict to alter in place
            depending on the iteration.
        frameworks (Tuple[str]): A list/tuple of the frameworks to be tested.
            Allowed are: "tf", "tfe", and "torch".
        session (bool): If True, enter a tf.Session() and yield that as
            well in the tf-case (otherwise, yield (fw, None)).

    Yields:
        str: If enter_session is False:
            The current framework ("tf", "tfe", "torch") used.
        Tuple(str, Union[None,tf.Session]: If enter_session is True:
            A tuple of the current fw and the tf.Session if fw="tf".
    """
    config = config or {}
    frameworks = [frameworks] if isinstance(frameworks, str) else frameworks

    for fw in frameworks:
        # Skip non-installed frameworks.
        if fw == "torch" and not torch:
            logger.warning(
                "framework_iterator skipping torch (not installed)!")
            continue
        if fw != "torch" and not tf:
            logger.warning("framework_iterator skipping {} (tf not "
                           "installed)!".format(fw))
            continue
        elif fw == "tfe" and not eager_mode:
            logger.warning("framework_iterator skipping eager (could not "
                           "import `eager_mode` from tensorflow.python)!")
            continue
        assert fw in ["tf", "tfe", "torch", None]

        # Do we need a test session?
        sess = None
        if fw == "tf" and session is True:
            sess = tf.Session()
            sess.__enter__()

        print("framework={}".format(fw))

        config["framework"] = fw

        eager_ctx = None
        if fw == "tfe":
            eager_ctx = eager_mode()
            eager_ctx.__enter__()
            assert tf.executing_eagerly()
        elif fw == "tf":
            assert not tf.executing_eagerly()

        yield fw if session is False else (fw, sess)

        # Exit any context we may have entered.
        if eager_ctx:
            eager_ctx.__exit__(None, None, None)
        elif sess:
            sess.__exit__(None, None, None)


def check(x, y, decimals=5, atol=None, rtol=None, false=False):
    """
    Checks two structures (dict, tuple, list,
    np.array, float, int, etc..) for (almost) numeric identity.
    All numbers in the two structures have to match up to `decimal` digits
    after the floating point. Uses assertions.

    Args:
        x (any): The value to be compared (to the expectation: `y`). This
            may be a Tensor.
        y (any): The expected value to be compared to `x`. This must not
            be a Tensor.
        decimals (int): The number of digits after the floating point up to
            which all numeric values have to match.
        atol (float): Absolute tolerance of the difference between x and y
            (overrides `decimals` if given).
        rtol (float): Relative tolerance of the difference between x and y
            (overrides `decimals` if given).
        false (bool): Whether to check that x and y are NOT the same.
    """
    # A dict type.
    if isinstance(x, dict):
        assert isinstance(y, dict), \
            "ERROR: If x is dict, y needs to be a dict as well!"
        y_keys = set(x.keys())
        for key, value in x.items():
            assert key in y, \
                "ERROR: y does not have x's key='{}'! y={}".format(key, y)
            check(
                value,
                y[key],
                decimals=decimals,
                atol=atol,
                rtol=rtol,
                false=false)
            y_keys.remove(key)
        assert not y_keys, \
            "ERROR: y contains keys ({}) that are not in x! y={}".\
            format(list(y_keys), y)
    # A tuple type.
    elif isinstance(x, (tuple, list)):
        assert isinstance(y, (tuple, list)),\
            "ERROR: If x is tuple, y needs to be a tuple as well!"
        assert len(y) == len(x),\
            "ERROR: y does not have the same length as x ({} vs {})!".\
            format(len(y), len(x))
        for i, value in enumerate(x):
            check(
                value,
                y[i],
                decimals=decimals,
                atol=atol,
                rtol=rtol,
                false=false)
    # Boolean comparison.
    elif isinstance(x, (np.bool_, bool)):
        if false is True:
            assert bool(x) is not bool(y), \
                "ERROR: x ({}) is y ({})!".format(x, y)
        else:
            assert bool(x) is bool(y), \
                "ERROR: x ({}) is not y ({})!".format(x, y)
    # Nones or primitives.
    elif x is None or y is None or isinstance(x, (str, int)):
        if false is True:
            assert x != y, "ERROR: x ({}) is the same as y ({})!".format(x, y)
        else:
            assert x == y, \
                "ERROR: x ({}) is not the same as y ({})!".format(x, y)
    # String comparison.
    elif hasattr(x, "dtype") and x.dtype == np.object:
        try:
            np.testing.assert_array_equal(x, y)
            if false is True:
                assert False, \
                    "ERROR: x ({}) is the same as y ({})!".format(x, y)
        except AssertionError as e:
            if false is False:
                raise e
    # Everything else (assume numeric or tf/torch.Tensor).
    else:
        if tf is not None:
            # y should never be a Tensor (y=expected value).
            if isinstance(y, tf.Tensor):
                raise ValueError("`y` (expected value) must not be a Tensor. "
                                 "Use numpy.ndarray instead")
            if isinstance(x, tf.Tensor):
                # In eager mode, numpyize tensors.
                if tf.executing_eagerly():
                    x = x.numpy()
                # Otherwise, use a quick tf-session.
                else:
                    with tf.Session() as sess:
                        x = sess.run(x)
                        return check(
                            x,
                            y,
                            decimals=decimals,
                            atol=atol,
                            rtol=rtol,
                            false=false)
        if torch is not None:
            if isinstance(x, torch.Tensor):
                x = x.detach().numpy()
            if isinstance(y, torch.Tensor):
                y = y.detach().numpy()

        # Using decimals.
        if atol is None and rtol is None:
            # Assert equality of both values.
            try:
                np.testing.assert_almost_equal(x, y, decimal=decimals)
            # Both values are not equal.
            except AssertionError as e:
                # Raise error in normal case.
                if false is False:
                    raise e
            # Both values are equal.
            else:
                # If false is set -> raise error (not expected to be equal).
                if false is True:
                    assert False, \
                        "ERROR: x ({}) is the same as y ({})!".format(x, y)

        # Using atol/rtol.
        else:
            # Provide defaults for either one of atol/rtol.
            if atol is None:
                atol = 0
            if rtol is None:
                rtol = 1e-7
            try:
                np.testing.assert_allclose(x, y, atol=atol, rtol=rtol)
            except AssertionError as e:
                if false is False:
                    raise e
            else:
                if false is True:
                    assert False, \
                        "ERROR: x ({}) is the same as y ({})!".format(x, y)


def check_learning_achieved(tune_results, min_reward):
    """Throws an error if `min_reward` is not reached within tune_results.

    Checks the last iteration found in tune_results for its
    "episode_reward_mean" value and compares it to `min_reward`.

    Args:
        tune_results: The tune.run returned results object.
        min_reward (float): The min reward that must be reached.

    Throws:
        ValueError: If `min_reward` not reached.
    """
    if tune_results.trials[0].last_result["episode_reward_mean"] < min_reward:
        raise ValueError("`stop-reward` of {} not reached!".format(min_reward))
    print("ok")


def check_compute_action(trainer,
                         include_state=False,
                         include_prev_action_reward=False):
    """Tests different combinations of arguments for trainer.compute_action.

    Args:
        trainer (Trainer): The trainer object to test.
        include_prev_action_reward (bool): Whether to include the prev-action
            and -reward in the `compute_action` call.

    Throws:
        ValueError: If anything unexpected happens.
    """
    try:
        pol = trainer.get_policy()
    except AttributeError:
        pol = trainer.policy

    obs_space = pol.observation_space
    action_space = pol.action_space
    for explore in [True, False]:
        for full_fetch in [True, False]:
            obs = np.clip(obs_space.sample(), -1.0, 1.0)
            state_in = None
            if include_state:
                state_in = pol.model.get_initial_state()
            action_in = action_space.sample() \
                if include_prev_action_reward else None
            reward_in = 1.0 if include_prev_action_reward else None
            out = trainer.compute_action(
                obs,
                state=state_in,
                prev_action=action_in,
                prev_reward=reward_in,
                explore=explore,
                full_fetch=full_fetch)

            state_out = None
            if state_in or full_fetch:
                action, state_out, _ = out
            if state_out:
                for si, so in zip(state_in, state_out):
                    check(list(si.shape), so.shape)

            if not action_space.contains(action):
                raise ValueError(
                    "Returned action ({}) of trainer {} not in Env's "
                    "action_space ({})!".format(action, trainer, action_space))
