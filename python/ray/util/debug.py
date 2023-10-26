from collections import defaultdict, namedtuple
import gc
import os
import re
import time
import tracemalloc
from typing import Callable, List, Optional
from ray.util.annotations import DeveloperAPI

_logged = set()
_disabled = False
_periodic_log = False
_last_logged = 0.0


@DeveloperAPI
def log_once(key):
    """Returns True if this is the "first" call for a given key.

    Various logging settings can adjust the definition of "first".

    Example:

        .. testcode::

            import logging
            from ray.util.debug import log_once

            logger = logging.getLogger(__name__)
            if log_once("some_key"):
                logger.info("Some verbose logging statement")
    """

    global _last_logged

    if _disabled:
        return False
    elif key not in _logged:
        _logged.add(key)
        _last_logged = time.time()
        return True
    elif _periodic_log and time.time() - _last_logged > 60.0:
        _logged.clear()
        _last_logged = time.time()
        return False
    else:
        return False


@DeveloperAPI
def disable_log_once_globally():
    """Make log_once() return False in this process."""

    global _disabled
    _disabled = True


@DeveloperAPI
def enable_periodic_logging():
    """Make log_once() periodically return True in this process."""

    global _periodic_log
    _periodic_log = True


@DeveloperAPI
def reset_log_once(key):
    """Resets log_once for the provided key."""

    _logged.discard(key)


# A suspicious memory-allocating stack-trace that we should re-test
# to make sure it's not a false positive.
Suspect = DeveloperAPI(
    namedtuple(
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
)


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
            # Manually trigger garbage collection before and after code runs in order to
            # make tracemalloc snapshots as accurate as possible.
            gc.collect()
            code()
            gc.collect()
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
    import scipy.stats
    import numpy as np

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
            (line.slope > 60.0 and line.rvalue > 0.875)
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
