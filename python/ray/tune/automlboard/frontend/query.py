from django.shortcuts import HttpResponse

from ray.tune.automlboard.models.models import JobRecord, TrialRecord
from ray.tune.trial import Trial

import json


def query_job(request):
    """Rest API to query the job info, with the given job_id.

    The url pattern should be like this:

    curl http://<server>:<port>/query_job?job_id=<job_id>

    The response may be:

    {
        "running_trials": 0,
        "start_time": "2018-07-19 20:49:40",
        "current_round": 1,
        "failed_trials": 0,
        "best_trial_id": "2067R2ZD",
        "name": "asynchyperband_test",
        "job_id": "asynchyperband_test",
        "user": "Grady",
        "type": "RAY TUNE",
        "total_trials": 4,
        "end_time": "2018-07-19 20:50:10",
        "progress": 100,
        "success_trials": 4
    }
    """
    job_id = request.GET.get("job_id")
    jobs = JobRecord.objects.filter(job_id=job_id)
    trials = TrialRecord.objects.filter(job_id=job_id)

    total_num = len(trials)
    running_num = sum(t.trial_status == Trial.RUNNING for t in trials)
    success_num = sum(t.trial_status == Trial.TERMINATED for t in trials)
    failed_num = sum(t.trial_status == Trial.ERROR for t in trials)
    if total_num == 0:
        progress = 0
    else:
        progress = int(float(success_num) / total_num * 100)

    if len(jobs) == 0:
        resp = "Unkonwn job id %s.\n" % job_id
    else:
        job = jobs[0]
        result = {
            "job_id": job.job_id,
            "name": job.name,
            "user": job.user,
            "type": job.type,
            "start_time": job.start_time,
            "end_time": job.end_time,
            "success_trials": success_num,
            "failed_trials": failed_num,
            "running_trials": running_num,
            "total_trials": total_num,
            "best_trial_id": job.best_trial_id,
            "progress": progress,
        }
        resp = json.dumps(result)
    return HttpResponse(resp, content_type="application/json;charset=utf-8")


def query_trial(request):
    """Rest API to query the trial info, with the given trial_id.

    The url pattern should be like this:

    curl http://<server>:<port>/query_trial?trial_id=<trial_id>

    The response may be:

    {
        "app_url": "None",
        "trial_status": "TERMINATED",
        "params": {'a': 1, 'b': 2},
        "job_id": "asynchyperband_test",
        "end_time": "2018-07-19 20:49:44",
        "start_time": "2018-07-19 20:49:40",
        "trial_id": "2067R2ZD",
    }
    """
    trial_id = request.GET.get("trial_id")
    trials = TrialRecord.objects.filter(trial_id=trial_id).order_by("-start_time")
    if len(trials) == 0:
        resp = "Unkonwn trial id %s.\n" % trials
    else:
        trial = trials[0]
        result = {
            "trial_id": trial.trial_id,
            "job_id": trial.job_id,
            "trial_status": trial.trial_status,
            "start_time": trial.start_time,
            "end_time": trial.end_time,
            "params": trial.params,
        }
        resp = json.dumps(result)
    return HttpResponse(resp, content_type="application/json;charset=utf-8")
