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
            job_id=json_info["job_id"],
            name=json_info["job_name"],
            user=json_info["user"],
            type=json_info["type"],
            start_time=json_info["start_time"],
            success_trials=json_info["success_trials"],
            failed_trials=json_info["failed_trials"],
            running_trials=json_info["running_trials"],
            total_trials=json_info["total_trials"],
            progress=json_info["progress"]
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
            trial_id=json_info["trial_id"],
            job_id=json_info["job_id"],
            trial_status=json_info["status"],
            start_time=json_info["start_time"],
            params=json_info["params"]
        )


class ResultRecord(models.Model):
    """
    Information of a single result of a trial
    """
    trial_id = models.CharField(max_length=50)
    timesteps_total = models.BigIntegerField(blank=True, null=True)
    done = models.CharField(max_length=30, blank=True, null=True)
    info = models.CharField(max_length=256, blank=True, null=True)
    episode_reward_mean = models.CharField(
        max_length=30, blank=True, null=True)
    episode_len_mean = models.CharField(
        max_length=30, blank=True, null=True)
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
        if json_info is None:
            return None
        return ResultRecord(
            trial_id=json_info["trial_id"],
            timesteps_total=json_info["timesteps_total"],
            done=json_info["done"],
            info=json_info["info"],
            episode_reward_mean=json_info["episode_reward_mean"],
            episodes_total=json_info["episodes_total"],
            mean_accuracy=json_info["mean_accuracy"],
            mean_validation_accuracy=json_info["mean_validation_accuracy"],
            mean_loss=json_info["mean_loss"],
            experiment_id=json_info["experiment_id"],
            trainning_iteration=json_info["training_iteration"],
            timesteps_this_iter=json_info["timesteps_this_iter"],
            time_this_iter_s=json_info["time_this_iter_s"],
            time_total_s=json_info["time_total_s"],
            date=json_info["date"],
            timestamp=json_info["timestamp"],
            hostname=json_info["hostname"],
            node_ip=json_info["node_ip"],
            config=json_info["config"])
