from anyscale.job.models import JobConfig
import anyscale
import os

command = "python anyscale_job_wrapper.py 'python hello_world.py' --test-workload-timeout 1800 --results-cloud-storage-uri 's3://ray-release-automation-results/working_dirs/hello_world.aks/abakvbwzfv/tmp/release_test_out.json' --metrics-cloud-storage-uri 's3://ray-release-automation-results/working_dirs/hello_world.aks/abakvbwzfv/tmp/metrics_test_out.json' --output-cloud-storage-uri 's3://ray-release-automation-results/working_dirs/hello_world.aks/abakvbwzfv/tmp/output.json' --upload-cloud-storage-uri 's3://ray-release-automation-results/working_dirs/hello_world.aks/abakvbwzfv' --prepare-commands --prepare-commands-timeouts"
job_config = JobConfig(
    name="hello_world_test",
    project="prj_y8syktydl7ltabhz5axdelwnce",
    cloud="cld_5nnv7pt2jn2312x2e5v72z53n2",
    image_uri="rayreleasetest.azurecr.io/anyscale/ray:abfss-adlfs-rebase",
    working_dir=os.getcwd(),
    entrypoint=command,
    compute_config="default_y8sy__compute__hello_world.azure__73898838c837ef6780e2965d7fa3a6f6a316b7c89480d1198084cddb3524efdb:1",
    env_vars={
        "BUILDKITE_BRANCH": "main",
    },
)
job_id = anyscale.job.submit(job_config)
