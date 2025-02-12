def _get_learner_bundles(config):
    if config.num_learners == 0:
        if config.num_aggregator_actors_per_learner > 0:
            return [{"CPU": config.num_aggregator_actors_per_learner}]
        else:
            return []

    num_cpus_per_learner = (
        config.num_cpus_per_learner
        if config.num_cpus_per_learner != "auto"
        else 1
        if config.num_gpus_per_learner == 0
        else 0
    )

    per_learner = {
        "CPU": (num_cpus_per_learner + config.num_aggregator_actors_per_learner),
        "GPU": config.num_gpus_per_learner,
    }

    return [per_learner] * config.num_learners
