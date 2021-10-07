from collections import defaultdict
import numpy as np
import re
import scipy
import tracemalloc
import tree  # pip install dm_tree

from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID


def check_memory_leaks(trainer, to_check=None, max_num_trials=3) -> None:
    """Diagnoses the given trainer for possible memory leaks.

    Isolates single components inside the trainer's local worker, e.g. the env,
    policy, etc.. and calls some of their methods repeatedly, while checking
    the memory footprints and keeping track of which lines in the code add
    un-GC'd items to memory.

    Args:
        trainer:
        to_test:

    Raises:
        MemoryError: If a memory leak in one of the components is found.
    """
    # Store up to n frames of each call stack.
    tracemalloc.start(10)

    local_worker = trainer.workers.local_worker()

    # Which components should we test?
    to_check = to_check or {"env", "model", "policy", "rollout_worker"}

    def _i_print(i):
        if (i + 1) % 10 == 0:
            print(".", end="" if (i + 1) % 100 else f" {i + 1}\n", flush=True)

    def _test(desc, init, func, repeats, max_num_trials):
        # Do n trials to make sure a found leak is really one.
        suspicious = None
        for trial in range(max_num_trials):
            table = defaultdict(list)

            # Play n episodes and check for leaks.
            actual_repeats = repeats * (trial + 1)

            print(desc, end="")
            print(f" {actual_repeats} times.")

            if init is not None:
                init()
            for i in range(actual_repeats):
                _i_print(i)
                func()
                _take_snapshot(table, suspicious)
            print("\n")
            # Check, which traces have moved up constantly over time.
            suspicious = set()
            try:
                _find_memory_leaks_in_table(table)
            except MemoryError as e:
                if trial == max_num_trials - 1:
                    raise e
                else:
                    print("Found suspicious memory allocation in traceback:")
                    for f in e.args[1].format():
                        print(f)
                    print(f"Increase total={e.args[2]}")
                    print(f"Slope={e.args[3]}")
                    print(f"Rval={e.args[4]}")
                    print("-> added to retry list")
                    suspicious.add(e.args[1])
            # Nothing suspicious found.
            if len(suspicious) == 0:
                break

    # Test a single sub-env (first in the VectorEnv)?
    if "env" in to_check:
        assert local_worker.async_env is not None, \
            "ERROR: Cannot test 'env' since given trainer does not have one " \
            "in its local worker. Try setting `create_env_on_driver=True`."

        # Isolate the first sub-env in the vectorized setup and test it.
        env = local_worker.async_env.get_unwrapped()[0]
        action_space = env.action_space

        def func():
            while True:
                action = action_space.sample()
                # If masking is used, try something like this:
                # np.random.choice(
                #    action_space.n, p=(obs["action_mask"] / sum(obs["action_mask"])))
                _, _, done, _ = env.step(action)
                if done:
                    env.reset()
                    break

        _test(
            desc="Looking for leaks in env, running through episodes.",
            init=lambda: env.reset(),
            func=func,
            # How many times to repeat the function call?
            repeats=200,
            max_num_trials=max_num_trials,
        )

    # Test the policy (single-agent case only so far).
    if "policy" in to_check:
        policy = local_worker.policy_map[DEFAULT_POLICY_ID]

        # Get a fixed obs.
        obs = tree.map_structure(
            lambda s: s[None],
            policy.observation_space.sample())

        print("Looking for leaks in Policy")

        def func():
            policy.compute_actions_from_input_dict({
                "obs": obs,
            })

        # Call `compute_actions_from_input_dict()` n times.
        _test(
            desc=f"Calling `compute_actions_from_input_dict()`.",
            init=None,
            func=func,
            # How many times to repeat the function call?
            repeats=400,
            # How many times to re-try if we find a suspicious memory
            # allocation?
            max_num_trials=max_num_trials,
        )

        # Call `learn_on_batch()` n times.
        dummy_batch = policy._get_dummy_batch_from_view_requirements(
            batch_size=16)

        def func():
            policy.learn_on_batch(dummy_batch)

        _test(
            desc=f"Calling `learn_on_batch()`.",
            init=None,
            func=func,
            # How many times to repeat the function call?
            repeats=100,
            max_num_trials=max_num_trials,
        )


def _take_snapshot(table, suspicious=None):
    # Take a memory snapshot.
    snapshot = tracemalloc.take_snapshot()
    # Group all memory allocations by their stacktrace (going n frames
    # deep as defined above in tracemalloc.start(n)).
    # Then sort groups by size, then count, then trace.
    top_stats = snapshot.statistics("traceback")

    # Loop only through n largest sizes and store these in our table.
    # Discard the rest.
    for stat in top_stats[:100]:
        if suspicious is None or stat.traceback in suspicious:
            table[stat.traceback].append(stat.size)


def _find_memory_leaks_in_table(table):
    for traceback, hist in table.items():
        top_stack = str(traceback[-1])
        # Do a quick mem increase check.
        memory_increase = hist[-1] - hist[0]
        # Only if memory increased, do we check further.
        if memory_increase > 0.0:
            # Ignore this very module here (we are collecting lots of data
            # so an increase is expected).
            if any(s in top_stack for s in [
                "tracemalloc", "pycharm", "thirdparty_files/psutil",
                re.sub("\\.", "/", __name__) + ".py"
            ]):
                continue

            # Do a linear regression to get the slope and R-value.
            line = scipy.stats.linregress(x=np.arange(len(hist)), y=np.array(hist))
            # - If weak positive slope and some confidence and
            #   increase > 100 bytes -> error.
            # - If stronger positive slope -> error.
            # deltas = np.array([0.0 if i == 0 else h - hist[i - 1] for i, h in
            #          enumerate(hist)])
            if memory_increase > 20 and (line.slope > 0.03 or (
                    line.slope > 0.0 and line.rvalue > 0.2)):
                raise MemoryError(
                    f"Found a memory leak inside {traceback}!\n"
                    f"increase={memory_increase}\n"
                    f"slope={line.slope}\n"
                    f"R={line.rvalue}\n"
                    f"hist={hist}", traceback, memory_increase, line.slope, line.rvalue)
