from ray.job_submission import JobSubmissionClient

client = JobSubmissionClient("http://127.0.0.1:8265")

kick_off_xgboost_benchmark = (
    # Clone ray. If ray is already present, don't clone again.
    "git clone https://github.com/ray-project/ray || true;"
    # Run the benchmark.
    " python ray/release/air_tests/air_benchmarks/workloads/xgboost_benchmark.py"
    " --size 100G --disable-check"
)


submission_id = client.submit_job(
    entrypoint=kick_off_xgboost_benchmark,
)

print("Use the following command to follow this Job's logs:")
print(f"ray job logs '{submission_id}' --follow")
