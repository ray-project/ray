from collections import defaultdict, namedtuple
import numpy as np
import os
import re
import scipy
import tracemalloc
import tree  # pip install dm_tree
from typing import Callable, DefaultDict, List, Optional, Set

from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch


# A suspicious memory-allocating stack-trace that we should re-test
# to make sure it's not a false positive.
Suspect = namedtuple(
    "Suspect",
    [
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
    ],
)


def check_memory_leaks(
    trainer,
    to_check: Optional[Set[str]] = None,
    repeats: Optional[int] = None,
    max_num_trials: int = 3,
) -> DefaultDict[str, List[Suspect]]:
    """Diagnoses the given trainer for possible memory leaks.

    Isolates single components inside the trainer's local worker, e.g. the env,
    policy, etc.. and calls some of their methods repeatedly, while checking
    the memory footprints and keeping track of which lines in the code add
    un-GC'd items to memory.

    Args:
        trainer: The Trainer instance to test.
        to_check: Set of strings to indentify components to test. Allowed strings
            are: "env", "policy", "model", "rollout_worker". By default, check all
            of these.
        repeats: Number of times the test code block should get executed (per trial).
            If a trial fails, a new trial may get started with a larger number of
            repeats: actual_repeats = `repeats` * (trial + 1) (1st trial == 0).
        max_num_trials: The maximum number of trials to run each check for.

    Raises:
        A defaultdict(list) with keys being the `to_check` strings and values being
        lists of Suspect instances that were found.
    """
    local_worker = trainer.workers.local_worker()

    # Which components should we test?
    to_check = to_check or {"env", "model", "policy", "rollout_worker"}

    results_per_category = defaultdict(list)

    # Test a single sub-env (first in the VectorEnv)?
    if "env" in to_check:
        assert local_worker.async_env is not None, (
            "ERROR: Cannot test 'env' since given trainer does not have one "
            "in its local worker. Try setting `create_env_on_driver=True`."
        )

        # Isolate the first sub-env in the vectorized setup and test it.
        env = local_worker.async_env.get_sub_environments()[0]
        action_space = env.action_space
        # Always use same action to avoid numpy random caused memory leaks.
        action_sample = action_space.sample()

        def code():
            env.reset()
            while True:
                # If masking is used, try something like this:
                # np.random.choice(
                #    action_space.n, p=(obs["action_mask"] / sum(obs["action_mask"])))
                _, _, done, _ = env.step(action_sample)
                if done:
                    break

        test = _test_some_code_for_memory_leaks(
            desc="Looking for leaks in env, running through episodes.",
            init=None,
            code=code,
            # How many times to repeat the function call?
            repeats=repeats or 200,
            max_num_trials=max_num_trials,
        )
        if test:
            results_per_category["env"].extend(test)

    # Test the policy (single-agent case only so far).
    if "policy" in to_check:
        policy = local_worker.policy_map[DEFAULT_POLICY_ID]

        # Get a fixed obs (B=10).
        obs = tree.map_structure(
            lambda s: np.stack([s] * 10, axis=0), policy.observation_space.sample()
        )

        print("Looking for leaks in Policy")

        def code():
            policy.compute_actions_from_input_dict(
                {
                    "obs": obs,
                }
            )

        # Call `compute_actions_from_input_dict()` n times.
        test = _test_some_code_for_memory_leaks(
            desc="Calling `compute_actions_from_input_dict()`.",
            init=None,
            code=code,
            # How many times to repeat the function call?
            repeats=repeats or 400,
            # How many times to re-try if we find a suspicious memory
            # allocation?
            max_num_trials=max_num_trials,
        )
        if test:
            results_per_category["policy"].extend(test)

        # Call `learn_on_batch()` n times.
        dummy_batch = policy._get_dummy_batch_from_view_requirements(batch_size=16)

        test = _test_some_code_for_memory_leaks(
            desc="Calling `learn_on_batch()`.",
            init=None,
            code=lambda: policy.learn_on_batch(dummy_batch),
            # How many times to repeat the function call?
            repeats=repeats or 100,
            max_num_trials=max_num_trials,
        )
        if test:
            results_per_category["policy"].extend(test)

    # Test only the model.
    if "model" in to_check:
        policy = local_worker.policy_map[DEFAULT_POLICY_ID]

        # Get a fixed obs.
        obs = tree.map_structure(lambda s: s[None], policy.observation_space.sample())

        print("Looking for leaks in Model")

        # Call `compute_actions_from_input_dict()` n times.
        test = _test_some_code_for_memory_leaks(
            desc="Calling `[model]()`.",
            init=None,
            code=lambda: policy.model({SampleBatch.OBS: obs}),
            # How many times to repeat the function call?
            repeats=repeats or 400,
            # How many times to re-try if we find a suspicious memory
            # allocation?
            max_num_trials=max_num_trials,
        )
        if test:
            results_per_category["model"].extend(test)

    # Test the RolloutWorker.
    if "rollout_worker" in to_check:
        print("Looking for leaks in local RolloutWorker")

        def code():
            local_worker.sample()
            local_worker.get_metrics()

        # Call `compute_actions_from_input_dict()` n times.
        test = _test_some_code_for_memory_leaks(
            desc="Calling `sample()` and `get_metrics()`.",
            init=None,
            code=code,
            # How many times to repeat the function call?
            repeats=repeats or 200,
            # How many times to re-try if we find a suspicious memory
            # allocation?
            max_num_trials=max_num_trials,
        )
        if test:
            results_per_category["rollout_worker"].extend(test)

    return results_per_category


def _test_some_code_for_memory_leaks(
    desc: str,
    init: Optional[Callable[[], None]],
    code: Callable[[], None],
    repeats: int,
    max_num_trials: int = 1,
) -> List[Suspect]:
    """Runs given code (and init code) n times and checks for memory leaks.

    Args:
        desc: A descriptor of the test.
        init: Optional code to be executed initially.
        code: The actual code to be checked for producing memory leaks.
        repeats: How many times to repeatedly execute `code`.
        max_num_trials: The maximum number of trials to run. A new trial is only
            run, if the previous one produced a memory leak. For all non-1st trials,
            `repeats` calculates as: actual_repeats = `repeats` * (trial + 1), where
            the first trial is 0.

    Returns:
        A list of Suspect objects, describing possible memory leaks. If list
        is empty, no leaks have been found.
    """

    def _i_print(i):
        if (i + 1) % 10 == 0:
            print(".", end="" if (i + 1) % 100 else f" {i + 1}\n", flush=True)

    # Do n trials to make sure a found leak is really one.
    suspicious = set()
    suspicious_stats = []
    for trial in range(max_num_trials):
        # Store up to n frames of each call stack.
        tracemalloc.start(20)

        table = defaultdict(list)

        # Repeat running code for n times.
        # Increase repeat value with each trial to make sure stats are more
        # solid each time (avoiding false positives).
        actual_repeats = repeats * (trial + 1)

        print(f"{desc} {actual_repeats} times.")

        # Initialize if necessary.
        if init is not None:
            init()
        # Run `code` n times, each time taking a memory snapshot.
        for i in range(actual_repeats):
            _i_print(i)
            code()
            _take_snapshot(table, suspicious)
        print("\n")

        # Check, which traces have moved up in their memory consumption
        # constantly over time.
        suspicious.clear()
        suspicious_stats.clear()
        # Suspicious memory allocation found?
        suspects = _find_memory_leaks_in_table(table)
        for suspect in sorted(suspects, key=lambda s: s.memory_increase, reverse=True):
            # Only print out the biggest offender:
            if len(suspicious) == 0:
                _pprint_suspect(suspect)
                print("-> added to retry list")
            suspicious.add(suspect.traceback)
            suspicious_stats.append(suspect)

        tracemalloc.stop()

        # Some suspicious memory allocations found.
        if len(suspicious) > 0:
            print(f"{len(suspicious)} suspects found. Top-ten:")
            for i, s in enumerate(suspicious_stats):
                if i > 10:
                    break
                print(
                    f"{i}) line={s.traceback[-1]} mem-increase={s.memory_increase}B "
                    f"slope={s.slope}B/detection rval={s.rvalue}"
                )
        # Nothing suspicious found -> Exit trial loop and return.
        else:
            print("No remaining suspects found -> returning")
            break

    # Print out final top offender.
    if len(suspicious_stats) > 0:
        _pprint_suspect(suspicious_stats[0])

    return suspicious_stats


def _take_snapshot(table, suspicious=None):
    # Take a memory snapshot.
    snapshot = tracemalloc.take_snapshot()
    # Group all memory allocations by their stacktrace (going n frames
    # deep as defined above in tracemalloc.start(n)).
    # Then sort groups by size, then count, then trace.
    top_stats = snapshot.statistics("traceback")

    # For the first m largest increases, keep only, if a) first trial or b) those
    # that are already in the `suspicious` set.
    for stat in top_stats[:100]:
        if not suspicious or stat.traceback in suspicious:
            table[stat.traceback].append(stat.size)


def _find_memory_leaks_in_table(table):
    suspects = []

    for traceback, hist in table.items():
        # Do a quick mem increase check.
        memory_increase = hist[-1] - hist[0]

        # Only if memory increased, do we check further.
        if memory_increase <= 0.0:
            continue

        # Ignore this very module here (we are collecting lots of data
        # so an increase is expected).
        top_stack = str(traceback[-1])
        drive_separator = "\\\\" if os.name == "nt" else "/"
        if any(
            s in top_stack
            for s in [
                "tracemalloc",
                "pycharm",
                "thirdparty_files/psutil",
                re.sub("\\.", drive_separator, __name__) + ".py",
            ]
        ):
            continue

        # Do a linear regression to get the slope and R-value.
        line = scipy.stats.linregress(x=np.arange(len(hist)), y=np.array(hist))

        # - If weak positive slope and some confidence and
        #   increase > n bytes -> error.
        # - If stronger positive slope -> error.
        if memory_increase > 1000 and (
            (line.slope > 40.0 and line.rvalue > 0.85)
            or (line.slope > 20.0 and line.rvalue > 0.9)
            or (line.slope > 10.0 and line.rvalue > 0.95)
        ):
            suspects.append(
                Suspect(
                    traceback=traceback,
                    memory_increase=memory_increase,
                    slope=line.slope,
                    rvalue=line.rvalue,
                    hist=hist,
                )
            )

    return suspects


def _pprint_suspect(suspect):
    print(
        "Most suspicious memory allocation in traceback "
        "(only printing out this one, but all (less suspicious)"
        " suspects will be investigated as well):"
    )
    print("\n".join(suspect.traceback.format()))
    print(f"Increase total={suspect.memory_increase}B")
    print(f"Slope={suspect.slope} B/detection")
    print(f"Rval={suspect.rvalue}")
