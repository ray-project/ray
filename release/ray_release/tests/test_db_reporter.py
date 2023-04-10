from ray_release.reporter.db import DBReporter


def test_compute_stack_pattern():
    assert (
        (DBReporter()).compute_crash_pattern(
            """
haha
Traceback (most recent call last):
    File "/tmp/something", line 584
Exception: yaya45
hehe
"""
        )
        == "somethingline Exception: yaya"
    )


def test_compute_unique_pattern():
    assert (DBReporter())._compute_unique_pattern(
        [
            "Traceback (most recent call last):",
            '   File "/tmp/something", line 584',
            "Exception: yaya45",
        ]
    ) == "somethingline Exception: yaya"


def test_compute_stack_trace():
    trace = [
        "Traceback (most recent call last):",
        '   File "/tmp/something", line 584, in run_release_test',
        "       raise pipeline_exception",
        "ray_release.exception.JobNoLogsError: Could not obtain logs for the job.",
    ]
    error_trace = [
        "[2023-01-01] ERROR: something is wrong" "Traceback (most recent call last):",
        '   File "/tmp/something", line 584, in run_release_test',
        "       raise pipeline_exception",
        "ray_release.exception.JobStartupTimeout: Cluster did not start.",
    ]
    error_trace_short = [
        "[2023-01-01] ERROR: something is wrong"
        '   File "/tmp/something", line 584, in run_release_test',
        "       raise pipeline_exception",
        "ray_release.exception.JobStartupTimeout: Cluster did not start.",
    ]
    assert (DBReporter())._compute_stack_trace(["haha"] + trace + ["hehe"]) == trace
    assert (DBReporter())._compute_stack_trace(
        ["haha"] + error_trace + ["hehe"]
    ) == error_trace
    assert (DBReporter())._compute_stack_trace(
        ["haha"] + error_trace_short + ["hehe"]
    ) == error_trace_short
    assert (DBReporter())._compute_stack_trace(
        ["haha"] + trace + ["w00t"] + error_trace + ["hehe"]
    ) == error_trace
