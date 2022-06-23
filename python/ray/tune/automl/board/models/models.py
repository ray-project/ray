from django.db import models


class JobRecord(models.Model):
    """Information of an AutoML Job."""

    job_id = models.CharField(max_length=50)
    name = models.CharField(max_length=20)
    user = models.CharField(max_length=20)
    type = models.CharField(max_length=20)
    start_time = models.CharField(max_length=50)
    end_time = models.CharField(max_length=50)
    best_trial_id = models.CharField(max_length=50)

    @classmethod
    def from_json(cls, json_info):
        """Build a Job instance from a json string."""
        if json_info is None:
            return None
        return JobRecord(
            job_id=json_info["job_id"],
            name=json_info["job_name"],
            user=json_info["user"],
            type=json_info["type"],
            start_time=json_info["start_time"],
        )

    def is_finished(self):
        """Judge whether this is a record for a finished job."""
        return self.end_time is not None


class TrialRecord(models.Model):
    """Information of a single AutoML trial of the job."""

    trial_id = models.CharField(max_length=50)
    job_id = models.CharField(max_length=50)
    trial_status = models.CharField(max_length=20)
    start_time = models.CharField(max_length=50)
    end_time = models.CharField(max_length=50)
    params = models.CharField(max_length=50, blank=True, null=True)
    metrics = models.CharField(max_length=256, null=True, blank=True)

    @classmethod
    def from_json(cls, json_info):
        """Build a Trial instance from a json string."""
        if json_info is None:
            return None
        return TrialRecord(
            trial_id=json_info["trial_id"],
            job_id=json_info["job_id"],
            trial_status=json_info["status"],
            start_time=json_info["start_time"],
            params=json_info["params"],
        )


class ResultRecord(models.Model):
    """Information of a single result of a trial."""

    trial_id = models.CharField(max_length=50)
    timesteps_total = models.BigIntegerField(blank=True, null=True)
    done = models.CharField(max_length=30, blank=True, null=True)
    episode_reward_mean = models.CharField(max_length=30, blank=True, null=True)
    mean_accuracy = models.FloatField(blank=True, null=True)
    mean_loss = models.FloatField(blank=True, null=True)
    trainning_iteration = models.BigIntegerField(blank=True, null=True)
    timesteps_this_iter = models.BigIntegerField(blank=True, null=True)
    time_this_iter_s = models.BigIntegerField(blank=True, null=True)
    time_total_s = models.BigIntegerField(blank=True, null=True)
    date = models.CharField(max_length=30, blank=True, null=True)
    hostname = models.CharField(max_length=50, blank=True, null=True)
    node_ip = models.CharField(max_length=50, blank=True, null=True)
    config = models.CharField(max_length=256, blank=True, null=True)

    @classmethod
    def from_json(cls, json_info):
        """Build a Result instance from a json string."""
        if json_info is None:
            return None
        return ResultRecord(
            trial_id=json_info["trial_id"],
            timesteps_total=json_info["timesteps_total"],
            done=json_info.get("done", None),
            episode_reward_mean=json_info.get("episode_reward_mean", None),
            mean_accuracy=json_info.get("mean_accuracy", None),
            mean_loss=json_info.get("mean_loss", None),
            trainning_iteration=json_info.get("training_iteration", None),
            timesteps_this_iter=json_info.get("timesteps_this_iter", None),
            time_this_iter_s=json_info.get("time_this_iter_s", None),
            time_total_s=json_info.get("time_total_s", None),
            date=json_info.get("date", None),
            hostname=json_info.get("hostname", None),
            node_ip=json_info.get("node_ip", None),
            config=json_info.get("config", None),
        )
