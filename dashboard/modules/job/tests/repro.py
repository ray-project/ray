from ray.job_submission import JobSubmissionClient

client = JobSubmissionClient()
job_id = client.submit_job(entrypoint="echo hi")
time.sleep(1)
client.stop_job(job_id)
client.delete_job(job_id)
print(client.list_jobs())
client.submit_job(entrypoint="echo hi again", submission_id=job_id)

