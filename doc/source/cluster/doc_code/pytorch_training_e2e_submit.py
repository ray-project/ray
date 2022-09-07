from ray.job_submission import JobSubmissionClient

client = JobSubmissionClient("http://127.0.0.1:8265")

kick_off_pytorch_benchmark = (
    # Clone ray. If ray is already present, don't clone again.
    "git clone https://github.com/ray-project/ray || true;"
    # Run the benchmark.
    "python ray/release/air_tests/air_benchmarks/workloads/pytorch_training_e2e.py"
    " --data-size-gb=1 --num-epochs=2 --num-workers=1"
)


submission_id = client.submit_job(
    entrypoint=kick_off_pytorch_benchmark,
)

print("Use the following command to follow this Job's logs:")
print(f"ray job logs '{submission_id}' --follow")
