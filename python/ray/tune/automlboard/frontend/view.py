from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from django.shortcuts import render

from models.models import JobRecord, TrialRecord, ResultRecord


def index(request):
    """View for the home page."""
    recent_jobs = JobRecord.objects.order_by('-start_time')[0:100]
    context = {'recent_jobs': recent_jobs}
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
