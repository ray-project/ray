import ray
from ray.job_submission import JobSubmissionClient
import pprint

ray.init()
client = JobSubmissionClient("http://localhost:8265")
job_id = client.submit_job(entrypoint="echo 'hello world'")
pprint.pprint(client.list_jobs(), width=1)
client.delete_job(job_id)
pprint.pprint(client.list_jobs())
#client.get_job_status(job_id)