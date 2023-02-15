import pytest
import subprocess
import sys

from pathlib import Path

import ray


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.parametrize("use_workaround", [False, True])
@pytest.mark.parametrize("exit_same", [False, True])
def test_registry_conflict(ray_start_4_cpus, tmpdir, use_workaround, exit_same):
    """Two concurrent Tune runs can conflict with each other when they
    use a trainable with the same name.

    This test starts two runs in parallel and asserts that a workaround used
    in the docs can alleviate the problem.

    This is how we schedule the runs:

    - We have two runs. Every run starts two trials.
    - Run 1 will start 1 trial immediately. This trial will be started with
      the correct parameters for the script. The trial hangs until the file
      ``run_2_finished`` is deleted.
    - Run 2 will start as soon as the 1st trial of Run 1 is running (by waiting
      until ``run_1_running`` file is deleted by that trial). It will overwrite
      the global registry trainable with the same name!
    - Run 2 will finish both trials. The script should finish with the expected
      parameters.
    - Run 2 will then delete the ``run_2_finished`` marker, allowing Run 1 trial 1
      to continue training. Training will finish, and the second trial will be launched.
      THIS TRIAL will then use the overwritten trainable, i.e. wrong parameters! At
      least unless the workaround is used.
    - Run 1 finally finishes, and we compare the expected results with the actual
      results.

    When no workaround is used, we expect an assertion error (if ``exit_same=True``,
    see below), otherwise a KeyError (because a trial failed).
    When the workaround is used, we expect everything to run fine.

    NOTE: There are two errors that can come up with registry conflicts: First,
    the trainable can be overwritten which will be captured e.g. when a fixed value
    is included in the trainable. The second trial of run 1 will then have a wrong
    parameter and report a wrong metric (from run 2).

    The second error comes up when the second run finishes fully and its objects
    are garbage collected. In this case, the first run tries to find the trainable
    registered by run 2, but this will fail lookup because the objects have been
    removed already. Note that these objects are the ones registered with
    ``tune.with_parameters()`` (not the global registry store).
    We test both scenarios using the ``exit_same`` parameter.

    NOTE: If we get around to resolving the registry issue (e.g. with unique keys)
    the test where we expect the assertion error can be removed! I.e. we can remove
    the parametrization and the workaround and just assert that no conflict comes up!
    """
    # Create file markers
    run_1_running = tmpdir / "run_1_running"
    run_1_finished = tmpdir / "run_1_finished"
    run_2_finished = tmpdir / "run_2_finished"

    run_1_running.write_text("", encoding="utf-8")
    run_1_finished.write_text("", encoding="utf-8")
    run_2_finished.write_text("", encoding="utf-8")

    ray_address = ray_start_4_cpus.address_info["address"]

    run_1_env = {
        "RAY_ADDRESS": ray_address,
        "WORKAROUND": str(int(use_workaround)),
        "FIXED_VAL": str(1),
        "VAL_1": str(2),
        "VAL_2": str(3),
        # Run 1 can start immediately
        "HANG_RUN_MARKER": "",
        # Allow second run to start once first trial of first run is started
        "DELETE_TRIAL_MARKER": str(run_1_running),
        # Hang in first trial until the second run finished
        "HANG_TRIAL_MARKER": str(run_2_finished),
        # Mark run 1 as completed
        "DELETE_RUN_MARKER": str(run_1_finished),
        # Do not wait at end
        "HANG_END_MARKER": "",
    }

    run_2_env = {
        "RAY_ADDRESS": ray_address,
        "WORKAROUND": str(int(use_workaround)),
        "FIXED_VAL": str(4),
        "VAL_1": str(5),
        "VAL_2": str(6),
        # Wait until first trial of first run is running
        "HANG_RUN_MARKER": str(run_1_running),
        # Don't delete during run
        "DELETE_TRIAL_MARKER": "",
        # No need to hang in trial
        "HANG_TRIAL_MARKER": "",
        # After full run finished, allow first run to continue
        "DELETE_RUN_MARKER": str(run_2_finished),
        # Wait until first run finished
        # If we don't do this, we actually don't die because of parameter conflict
        # but because of "The object's owner has exited" - so we test this
        # separately
        "HANG_END_MARKER": str(run_1_finished) if exit_same else "",
    }

    script_path = Path(__file__).parent / "_test_multi_tenancy_run.py"

    run_1 = subprocess.Popen(
        [sys.executable, script_path], env=run_1_env, stderr=subprocess.PIPE
    )
    print("Started run 1:", run_1.pid)

    run_2 = subprocess.Popen([sys.executable, script_path], env=run_2_env)
    print("Started run 2:", run_2.pid)

    assert run_2.wait() == 0

    if use_workaround:
        assert run_1.wait() == 0
    else:
        assert run_1.wait() != 0

        stderr = run_1.stderr.read().decode()

        if not exit_same:
            assert "OwnerDiedError" in stderr, stderr
        else:
            assert "AssertionError" in stderr, stderr


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
