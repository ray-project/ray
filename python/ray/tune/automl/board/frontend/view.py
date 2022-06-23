from django.shortcuts import render

from ray.tune.automl.board.settings import (
    AUTOMLBOARD_RELOAD_INTERVAL,
    AUTOMLBOARD_LOG_DIR,
)
from ray.tune.automl.board.models.models import JobRecord, TrialRecord, ResultRecord
from ray.tune.experiment import Trial

import datetime


def index(request):
    """View for the home page."""
    recent_jobs = JobRecord.objects.order_by("-start_time")[0:100]
    recent_trials = TrialRecord.objects.order_by("-start_time")[0:500]

    total_num = len(recent_trials)
    running_num = sum(t.trial_status == Trial.RUNNING for t in recent_trials)
    success_num = sum(t.trial_status == Trial.TERMINATED for t in recent_trials)
    failed_num = sum(t.trial_status == Trial.ERROR for t in recent_trials)

    job_records = []
    for recent_job in recent_jobs:
        job_records.append(get_job_info(recent_job))
    context = {
        "log_dir": AUTOMLBOARD_LOG_DIR,
        "reload_interval": AUTOMLBOARD_RELOAD_INTERVAL,
        "recent_jobs": job_records,
        "job_num": len(job_records),
        "trial_num": total_num,
        "running_num": running_num,
        "success_num": success_num,
        "failed_num": failed_num,
    }
    return render(request, "index.html", context)


def job(request):
    """View for a single job."""
    job_id = request.GET.get("job_id")
    recent_jobs = JobRecord.objects.order_by("-start_time")[0:100]
    recent_trials = TrialRecord.objects.filter(job_id=job_id).order_by("-start_time")
    trial_records = []
    for recent_trial in recent_trials:
        trial_records.append(get_trial_info(recent_trial))
    current_job = JobRecord.objects.filter(job_id=job_id).order_by("-start_time")[0]

    if len(trial_records) > 0:
        param_keys = trial_records[0]["params"].keys()
    else:
        param_keys = []

    # TODO: support custom metrics here
    metric_keys = ["episode_reward", "accuracy", "loss"]
    context = {
        "current_job": get_job_info(current_job),
        "recent_jobs": recent_jobs,
        "recent_trials": trial_records,
        "param_keys": param_keys,
        "param_num": len(param_keys),
        "metric_keys": metric_keys,
        "metric_num": len(metric_keys),
    }
    return render(request, "job.html", context)


def trial(request):
    """View for a single trial."""
    job_id = request.GET.get("job_id")
    trial_id = request.GET.get("trial_id")
    recent_trials = TrialRecord.objects.filter(job_id=job_id).order_by("-start_time")
    recent_results = ResultRecord.objects.filter(trial_id=trial_id).order_by("-date")[
        0:2000
    ]
    current_trial = TrialRecord.objects.filter(trial_id=trial_id).order_by(
        "-start_time"
    )[0]
    context = {
        "job_id": job_id,
        "trial_id": trial_id,
        "current_trial": current_trial,
        "recent_results": recent_results,
        "recent_trials": recent_trials,
    }
    return render(request, "trial.html", context)


def get_job_info(current_job):
    """Get job information for current job."""
    trials = TrialRecord.objects.filter(job_id=current_job.job_id)
    total_num = len(trials)
    running_num = sum(t.trial_status == Trial.RUNNING for t in trials)
    success_num = sum(t.trial_status == Trial.TERMINATED for t in trials)
    failed_num = sum(t.trial_status == Trial.ERROR for t in trials)

    if total_num == 0:
        progress = 0
    else:
        progress = int(float(success_num) / total_num * 100)

    winner = get_winner(trials)

    job_info = {
        "job_id": current_job.job_id,
        "job_name": current_job.name,
        "user": current_job.user,
        "type": current_job.type,
        "start_time": current_job.start_time,
        "end_time": current_job.end_time,
        "total_num": total_num,
        "running_num": running_num,
        "success_num": success_num,
        "failed_num": failed_num,
        "best_trial_id": current_job.best_trial_id,
        "progress": progress,
        "winner": winner,
    }

    return job_info


def get_trial_info(current_trial):
    """Get job information for current trial."""
    if current_trial.end_time and ("_" in current_trial.end_time):
        # end time is parsed from result.json and the format
        # is like: yyyy-mm-dd_hh-MM-ss, which will be converted
        # to yyyy-mm-dd hh:MM:ss here
        time_obj = datetime.datetime.strptime(
            current_trial.end_time, "%Y-%m-%d_%H-%M-%S"
        )
        end_time = time_obj.strftime("%Y-%m-%d %H:%M:%S")
    else:
        end_time = current_trial.end_time

    if current_trial.metrics:
        metrics = eval(current_trial.metrics)
    else:
        metrics = None

    trial_info = {
        "trial_id": current_trial.trial_id,
        "job_id": current_trial.job_id,
        "trial_status": current_trial.trial_status,
        "start_time": current_trial.start_time,
        "end_time": end_time,
        "params": eval(current_trial.params.encode("utf-8")),
        "metrics": metrics,
    }

    return trial_info


def get_winner(trials):
    """Get winner trial of a job."""
    winner = {}
    # TODO: sort_key should be customized here
    sort_key = "accuracy"
    if trials and len(trials) > 0:
        first_metrics = get_trial_info(trials[0])["metrics"]
        if first_metrics and not first_metrics.get("accuracy", None):
            sort_key = "episode_reward"
        max_metric = float("-Inf")
        for t in trials:
            metrics = get_trial_info(t).get("metrics", None)
            if metrics and metrics.get(sort_key, None):
                current_metric = float(metrics[sort_key])
                if current_metric > max_metric:
                    winner["trial_id"] = t.trial_id
                    winner["metric"] = sort_key + ": " + str(current_metric)
                    max_metric = current_metric
    return winner
