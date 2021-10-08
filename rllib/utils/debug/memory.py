from collections import defaultdict, namedtuple
import gc
import numpy as np
import re
import scipy
import tracemalloc
import tree  # pip install dm_tree

from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID


# A suspicious memory-allocating stack-trace that we should re-test
# to make sure it's not a false positive.
Suspect = namedtuple("Suspect", [
    # The stack trace of the allocation, going back n frames, depending
    # on the tracemalloc.start(n) call.
    "traceback",
    # The amount of memory taken by this particular stack trace
    # over the course of the experiment.
    "memory_increase",
    # The slope of the scipy linear regression (x=iteration; y=memory size).
    "slope",
    # The rvalue of the scipy linear regression.
    "rvalue",
    # The memory size history (list of all memory sizes over all iterations).
    "hist",
])


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
    local_worker = trainer.workers.local_worker()

    # Which components should we test?
    to_check = to_check or {"env", "model", "policy", "rollout_worker"}

    # Test a single sub-env (first in the VectorEnv)?
    if "env" in to_check:
        assert local_worker.async_env is not None, \
            "ERROR: Cannot test 'env' since given trainer does not have one " \
            "in its local worker. Try setting `create_env_on_driver=True`."

        # Isolate the first sub-env in the vectorized setup and test it.
        env = local_worker.async_env.get_unwrapped()[0]
        action_space = env.action_space

        def code():
            while True:
                action = action_space.sample()
                # If masking is used, try something like this:
                # np.random.choice(
                #    action_space.n, p=(obs["action_mask"] / sum(obs["action_mask"])))
                _, _, done, _ = env.step(action)
                if done:
                    env.reset()
                    break

        _test_some_code_for_memory_leaks(
            desc="Looking for leaks in env, running through episodes.",
            init=lambda: env.reset(),
            code=code,
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

        def code():
            policy.compute_actions_from_input_dict({
                "obs": obs,
            })

        # Call `compute_actions_from_input_dict()` n times.
        _test_some_code_for_memory_leaks(
            desc=f"Calling `compute_actions_from_input_dict()`.",
            init=None,
            code=code,
            # How many times to repeat the function call?
            repeats=400,
            # How many times to re-try if we find a suspicious memory
            # allocation?
            max_num_trials=max_num_trials,
        )

        # Call `learn_on_batch()` n times.
        dummy_batch = policy._get_dummy_batch_from_view_requirements(
            batch_size=16)

        _test_some_code_for_memory_leaks(
            desc=f"Calling `learn_on_batch()`.",
            init=None,
            code=lambda: policy.learn_on_batch(dummy_batch),
            # How many times to repeat the function call?
            repeats=100,
            max_num_trials=max_num_trials,
        )

    # Test only the model.
    if "model" in to_check:
        policy = local_worker.policy_map[DEFAULT_POLICY_ID]

        # Get a fixed obs.
        obs = tree.map_structure(
            lambda s: s[None],
            policy.observation_space.sample())

        print("Looking for leaks in Model")

        # Call `compute_actions_from_input_dict()` n times.
        _test_some_code_for_memory_leaks(
            desc=f"Calling `[model]()`.",
            init=None,
            code=lambda: policy.model.base_model(obs),#TODO: note every model may have a base_model!
            # How many times to repeat the function call?
            repeats=400,
            # How many times to re-try if we find a suspicious memory
            # allocation?
            max_num_trials=max_num_trials,
        )

    # Test the RolloutWorker.
    if "rollout_worker" in to_check:
        print("Looking for leaks in local RolloutWorker")

        # Call `compute_actions_from_input_dict()` n times.
        _test_some_code_for_memory_leaks(
            desc=f"Calling `sample()`.",
            init=None,
            code=lambda: local_worker.sample(),
            # How many times to repeat the function call?
            repeats=400,
            # How many times to re-try if we find a suspicious memory
            # allocation?
            max_num_trials=max_num_trials,
        )


def _test_some_code_for_memory_leaks(desc, init, code, repeats, max_num_trials):

    def _i_print(i):
        if (i + 1) % 10 == 0:
            print(".", end="" if (i + 1) % 100 else f" {i + 1}\n", flush=True)

    # Do n trials to make sure a found leak is really one.
    suspicious = None
    for trial in range(max_num_trials):
        # Store up to n frames of each call stack.
        tracemalloc.start(10)

        table = defaultdict(list)

        # Repeat running code for n times.
        # Increase repeat value with each trial to make sure stats are more
        # solid each time (avoiding false positives).
        actual_repeats = repeats * (trial + 1)

        print(desc, end="")
        print(f" {actual_repeats} times.")

        # Initialize if necessary.
        if init is not None:
            init()
        # Run `code` n times, each time taking a memory snapshot.
        for i in range(actual_repeats):
            _i_print(i)
            code()
            _take_snapshot(table, suspicious)
        print("\n")

        # Check, which traces have moved up constantly over time.
        suspicious = set()
        suspicious_stats = []
        # Suspicious memory allocation found?
        suspects = _find_memory_leaks_in_table(table)
        for suspect in sorted(suspects, key=lambda s: s.memory_increase, reverse=True):
            pretty_traceback = "\n".join(suspect.traceback.format())

            # Reached max trials -> Error.
            if trial == max_num_trials - 1:
                raise MemoryError(
                    f"Memory leak in traceback:\n{pretty_traceback}!", suspect)
            # Start another trial (with more repeats), only looking at the
            # suspicious stack-traces this time.

            # Only print out the biggest offender:
            if len(suspicious) == 0:
                print("Most suspicious memory allocation in traceback "
                      "(only printing out this one, but all (less suspicious)"
                      " suspects will be investigated as well):")
                print(pretty_traceback)
                print(f"Increase total={suspect.memory_increase}B")
                print(f"Slope={suspect.slope} B/call")
                print(f"Rval={suspect.rvalue}")
                print("-> added to retry list")
            suspicious.add(suspect.traceback)
            suspicious_stats.append(suspect)

        tracemalloc.clear_traces()

        # Nothing suspicious found -> Exit trial loop and return.
        if len(suspicious) == 0:
            print("No remaining suspects found -> returning (ok)")
            return
        else:
            print(f"Suspects found: {len(suspicious)}. Top-ten:")
            for i, s in enumerate(sorted(suspicious_stats, key=lambda s: s.memory_increase, reverse=True)):
                if i > 10:
                    break
                print(f"{i}) line={s.traceback[-1]} mem-increase={s.memory_increase}B slope={s.slope}B/call rval={s.rvalue}")


def _take_snapshot(table, suspicious=None):
    # Take a memory snapshot.
    snapshot = tracemalloc.take_snapshot()
    # Group all memory allocations by their stacktrace (going n frames
    # deep as defined above in tracemalloc.start(n)).
    # Then sort groups by size, then count, then trace.
    top_stats = snapshot.statistics("traceback")

    # Always loop only through n largest sizes and store these in our
    # table, no matter what.
    for stat in top_stats[:100]:
        table[stat.traceback].append(stat.size)
    # For the next m largest sizes, keep if a) first trial or b) those
    # that are already in the `suspicious` set.
    for stat in top_stats[100:1000]:
        if suspicious is None or stat.traceback in suspicious:
            table[stat.traceback].append(stat.size)


def _find_memory_leaks_in_table(table):
    suspects = []

    for traceback, hist in table.items():
        # Do a quick mem increase check.
        memory_increase = hist[-1] - hist[0]
        # Only if memory increased, do we check further.
        if memory_increase > 0.0:
            # Ignore this very module here (we are collecting lots of data
            # so an increase is expected).
            top_stack = str(traceback[-1])
            if any(s in top_stack for s in [
                "tracemalloc", "pycharm", "thirdparty_files/psutil",
                re.sub("\\.", "/", __name__) + ".py"
            ]):
                continue

            # Do a linear regression to get the slope and R-value.
            line = scipy.stats.linregress(
                x=np.arange(len(hist)),
                y=np.array(hist))

            # - If weak positive slope and some confidence and
            #   increase > 100 bytes -> error.
            # - If stronger positive slope -> error.
            # deltas = np.array([0.0 if i == 0 else h - hist[i - 1] for i, h in
            #          enumerate(hist)])
            if memory_increase > 20 and (line.slope > 0.03 or (
                    line.slope > 0.0 and line.rvalue > 0.2)):
                suspects.append(Suspect(
                    traceback=traceback,
                    memory_increase=memory_increase,
                    slope=line.slope,
                    rvalue=line.rvalue,
                    hist=hist,
                ))

    return suspects
