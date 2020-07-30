storage = TrialStorage(location)
trials = storage.get_trials()
failed_trials = trials.filter(status=Failed)
parameters = [t.hypers for t in failed_trials]


# Builder Pattern

factory = TrialFactory()
factory.queue(grid)
run(func, factory)

factory = TrialFactory()
factory.queue(distribution, num_samples=3, repeat=5)
run(func, factory)

factory = TrialFactory(optimizer)
factory.queue(distribution, delay_feedback=3, num_samples=20, max_concurrent=3)
run(func, factory)

optimizer.restore(storage)

factory = TrialFactory(optimizer)
factory.queue(parameter_list)
factory.queue(distribution, num_samples=3)

# single process

trials = []

while factory.has_next():
    x = factory.next()
    trial = build(func, x)
    trials.put(trial)
    storage.save(factory, trials)

    while not trial.done():
        result = get_next_result(trial)
        log(result)
        storage.update_checkpoint(trial)
    factory.update(result)
    storage.save(factory, trials)


# concurrent

trials = []

class Actor:
    def __init__(self):
        pass

    def configure():
        pass

    def step():
        pass

    def save():
        pass

    def restore():
        pass

factory, trials = storage.recover()
optimizer = factory.optimizer
result_streams = []

while factory.has_next() or not trials.not_done():
    while factory.has_next():
        x = factory.next()
        trial = build(func, x)
        trials.put(trial)
        storage.save(factory, trials)

    while Cluster.has_space(trials.live()) and trials.has_pending():
        trial = trials.pop_pending()
        handle = Actor.configure(trial)
        result_streams.add(handle)

    trial, handle, payload = process_next(result_streams)

    if payload.type == "SAVE":
        trial.update(payload.checkpoint)
        storage.save(trials)
    elif payload.type == "STEP":
        trial.track(payload.result)
        log(payload.result)
    else:
        pass

    if should_checkpoint(trial):
        Executor.save(handle)
    elif not is_finished(trial):
        action = Scheduler(trial, trials)
        Executor.execute_action(action, trial)
    elif is_finished(trial):
        factory.update(trial, result)
        storage.save(factory, trials)



# concurrent with checkpointing



# concurrent with pbt

while factory.has_next() or not trials.not_done():
    # ...

    trial, handle, payload = process_next(result_streams)
    elif not is_finished(trial):
        action = pbt(trial, trials)
        factory.queue(new_hps, trial3.checkpoint)

        Executor.execute_action(action, trial)



# Restore last experiment
exp = Experiment.restore(storage=X)
trials = exp.get_trial(filter=failed)

run(func, manual_list)
run(func, space, searcher)
run(func, grid)
run(func, manual_list, checkpoints)
run(func, manual_list)




run(func, exp)


# Core concepts:
# Result: Dict[str, value]
# t_state: Any
# Trial: hps[Dict], static_config[Dict]
# TrialTrace: List[Result], t_state, Trial

# Trainable: t_state, Trial -> t_state, Result

# Optimizer: o_state, List[TrialTrace], Trainable -> (
# o_state, List[TrialTrace])

# 	SearchAlg: state, Dict[hps, Result] -> state, hps


# Execution concepts

# Checkpoint
# LiveTrial: TrialTrace, location, status, is_idle
# Status: PENDING, SAVING, RESTORING, TRAINING, SETUP, STOP, ERROR
# Trainer: Trainable, location, t_state, Trial -> t_state, Result
step(o_state, LiveTrial, List[LiveTrial]) -> LiveTrial, *args
	Server(List[LiveTrial]) -> List[LiveTrial]
	checkpointer(LiveTrial, manager_state) -> TrialTrace
	Logger(TrialTrace)
	Optimizer(o_trace, ...)
	Syncer()




TrialExecutor(reuse_actors, queue_trials)
ServerConfig(server_port)
Optimizer(stop, search_alg, scheduler)
Experiment(resume, local_dir)
CheckpointManager(
    sync_on_checkpoint,
    keep_checkpoints_num,
    global_checkpoint_period,
    export_formats,
    checkpoint_score_attr
)
### Tune commands
tune.set_log_config(
    upload_dir,
    sync_to_cloud,
    trial_name_creator,
    sync_to_driver,
    progress_reporter,
    loggers,
    verbose
)
tune.set_server(ServerConfig)
tune.run(
    experiment,
    trainable_fn,
    raise_on_failed_trial,   # where can this go?
    max_failures: int or "fail-fast",
    trial_executor,
    restore_from,  # checkpoint path to restore from
    resources_per_trial,
    num_samples,
    search_space, # I'm not a big fan of this because Search Algs have their own search_space too
    Optimizer,
    CheckpointManager)







