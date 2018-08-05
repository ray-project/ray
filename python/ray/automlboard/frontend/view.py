from __future__ import unicode_literals

from django.shortcuts import render

from models.models import JobRecord, TrialRecord, ResultRecord


def index(request):
    context = {'recent_jobs': JobRecord.objects.order_by('-start_time')[0:100],
               'recent_trials': TrialRecord.objects.order_by('-start_time')[0:500],
               'recent_metrics': ResultRecord.objects.order_by('-date')[0:2000]}
    return render(request, 'index.html', context)


def job(request):
    job_id = request.GET.get('job_id')
    context = {'job_id': job_id,
               'recent_trials': TrialRecord.objects.filter(job_id=job_id).order_by('-start_time')}
    return render(request, 'job.html', context)


def trial(request):
    job_id = request.GET.get('job_id')
    trial_id = request.GET.get('trial_id')
    context = {'job_id': job_id,
               'trial_id': trial_id,
               'recent_results': ResultRecord.objects.filter(trial_id=trial_id).order_by('-date')[0:2000]}
    return render(request, 'trial.html', context)
