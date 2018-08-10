from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from django.shortcuts import render

from models.models import JobRecord, TrialRecord, ResultRecord


def index(request):
    """View for the home page."""
    recent_jobs = JobRecord.objects.order_by('-start_time')[0:100]
    job_records = []
    for recent_job in recent_jobs:
        trials = TrialRecord.objects.filter(job_id=recent_job.job_id)
        total_num = len(trials)
        running_num = len(
            filter(lambda t: t.trial_status == "RUNNING", trials))
        success_num = len(
            filter(lambda t: t.trial_status == "TERMINATED", trials))
        failed_num = len(filter(lambda t: t.trial_status == "FAILED", trials))

        if total_num == 0:
            progress = 0
        else:
            progress = int(float(success_num) / total_num * 100)

        job_records.append({
            "job_id": recent_job.job_id,
            "job_name": recent_job.name,
            "user": recent_job.user,
            "type": recent_job.type,
            "start_time": recent_job.start_time,
            "end_time": recent_job.end_time,
            "total_num": total_num,
            "running_num": running_num,
            "success_num": success_num,
            "failed_num": failed_num,
            "best_trial_id": recent_job.best_trial_id,
            "progress": progress
        })
    context = {'recent_jobs': job_records}
    return render(request, 'index.html', context)


def job(request):
    """View for a single job."""
    job_id = request.GET.get('job_id')
    recent_trials = TrialRecord.objects \
        .filter(job_id=job_id) \
        .order_by('-start_time')
    context = {'job_id': job_id, 'recent_trials': recent_trials}
    return render(request, 'job.html', context)


def trial(request):
    """View for a single trial."""
    job_id = request.GET.get('job_id')
    trial_id = request.GET.get('trial_id')
    recent_results = ResultRecord.objects \
        .filter(trial_id=trial_id) \
        .order_by('-date')[0:2000]
    context = {
        'job_id': job_id,
        'trial_id': trial_id,
        'recent_results': recent_results
    }
    return render(request, 'trial.html', context)
