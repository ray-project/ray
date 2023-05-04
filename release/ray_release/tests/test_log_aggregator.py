from ray_release.log_aggregator import LogAggregator


def test_compute_stack_pattern():
    assert (
        LogAggregator(
            "\n".join(
                [
                    "haha",
                    "Traceback (most recent call last):",
                    '    File "/tmp/something", line 584',
                    "Exception: yaya45",
                    "hehe",
                ]
            )
        ).compute_crash_pattern()
        == "somethingline Exception: yaya"
    )


def test_compute_signature():
    assert (
        LogAggregator._compute_signature(
            [
                "Traceback (most recent call last):",
                '   File "/tmp/something", line 584',
                '   File "/tmp/another", deedeebeeaacfa-abc' "Exception: yaya45",
            ]
        )
        == "somethingline another-abcException: yaya"
    )


def test_compute_stack_trace():
    trace = [
        "Traceback (most recent call last):",
        '   File "/tmp/something", line 584, in run_release_test',
        "       raise pipeline_exception",
        "ray_release.exception.JobNoLogsError: Could not obtain logs for the job.",
    ]
    error_trace = [
        "[2023-01-01] ERROR: something is wrong",
        "Traceback (most recent call last):",
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
    assert LogAggregator._compute_stack_trace(["haha"] + trace + ["hehe"]) == trace
    assert (
        LogAggregator._compute_stack_trace(["haha"] + error_trace + ["hehe"])
        == error_trace
    )
    assert (
        LogAggregator._compute_stack_trace(["haha"] + error_trace_short + ["hehe"])
        == error_trace_short
    )
    assert (
        LogAggregator._compute_stack_trace(
            ["haha"] + trace + ["w00t"] + error_trace + ["hehe"]
        )
        == error_trace
    )
