from django.db import models


class JobRecord(models.Model):
    """
    Information of an AutoML Job
    """
    job_id = models.CharField(max_length=50)
    name = models.CharField(max_length=20)
    user = models.CharField(max_length=20)
    type = models.CharField(max_length=20)
    start_time = models.CharField(max_length=50)
    end_time = models.CharField(max_length=50)
    success_trials = models.BigIntegerField()
    failed_trials = models.BigIntegerField()
    running_trials = models.BigIntegerField()
    total_trials = models.BigIntegerField()
    best_trial_id = models.CharField(max_length=50)
    progress = models.BigIntegerField()

    @classmethod
    def from_json(cls, json_info):
        """
        Build a Job instance from a json string.
        """
        if json_info is None:
            return None
        return JobRecord(
            json_info["job_id"],
            json_info["job_name"],
            json_info["user"],
            json_info["type"],
            json_info["start_time"],
            json_info["end_time"],
            json_info["success_trials"],
            json_info["failed_trials"],
            json_info["running_trials"],
            json_info["total_trials"],
            json_info["best_trial_id"],
            json_info["progress"]
        )

    def is_finished(self):
        return self.end_time is not None


class TrialRecord(models.Model):
    """
    Information of a single AutoML trial of the job
    """
    trial_id = models.CharField(max_length=50)
    job_id = models.CharField(max_length=50)
    trial_status = models.CharField(max_length=20)
    start_time = models.CharField(max_length=50)
    end_time = models.CharField(max_length=50)
    params = models.CharField(max_length=50)

    @classmethod
    def from_json(cls, json_info):
        """
        Build a Trial instance from a json string.
        """
        if json_info is None:
            return None
        return TrialRecord(
            json_info["trial_id"],
            json_info["job_id"],
            json_info["status"],
            json_info["start_time"],
            json_info["end_time"],
            json_info["params"]
        )


class ResultRecord(models.Model):
    """
    Information of a single result of a trial
    """
    trial_id = models.CharField(max_length=50)
    timesteps_total = models.BigIntegerField(blank=True, null=True)
    done = models.CharField(max_length=30, blank=True, null=True)
    info = models.CharField(max_length=256, blank=True, null=True)
    episode_reward_mean = models.CharField(max_length=30, blank=True, null=True)
    episode_len_mean = models.CharField(max_length=30, blank=True, null=True)
    episodes_total = models.CharField(max_length=30, blank=True, null=True)
    mean_accuracy = models.FloatField(blank=True, null=True)
    mean_validation_accuracy = models.FloatField(blank=True, null=True)
    mean_loss = models.FloatField(blank=True, null=True)
    neg_mean_loss = models.FloatField(blank=True, null=True)
    experiment_id = models.CharField(max_length=256, blank=True, null=True)
    trainning_iteration = models.BigIntegerField(blank=True, null=True)
    timesteps_this_iter = models.BigIntegerField(blank=True, null=True)
    time_this_iter_s = models.BigIntegerField(blank=True, null=True)
    time_total_s = models.BigIntegerField(blank=True, null=True)
    date = models.CharField(max_length=30, blank=True, null=True)
    timestamp = models.BigIntegerField(blank=True, null=True)
    hostname = models.CharField(max_length=50, blank=True, null=True)
    node_ip = models.CharField(max_length=50, blank=True, null=True)
    config = models.CharField(max_length=256, blank=True, null=True)

    @classmethod
    def from_json(cls, json_info):
        """
        Build a Result instance from a json string.
        """
        # TODO: implement this method
        raise NotImplementedError
