class Trial:
	hypers: dict = {} # static
	config: dict = {} # static
	status: str = None
	trace: List[Dict] = []
	checkpoints: List[str] = []

space = {}
trials = []

trial_checkpoints = {}

while not Optimizer.is_finished():
	while Optimizer.has_next(space, trials, state):
		trials += [Optimizer.next(space, trials, state)]
	trial = Optimizer.choose(trials, state)
	if Optimizer.should_stop(trial, trials, state):
		Executor.stop(trial)
	elif Optimizer.should_pause(trial, state):
		Executor.pause(trial)
	elif Optimizer.should_restore(trial, state):
			restore(trial, trial.checkpoints[-1])
	elif Optimizer.should_save(trial, state):
			checkpoint = save(trial)
	elif Optimizer.should_continue(trial, state):
			step(trial)


exp = Experiment(logdir, name, restore=True)
failed_trials = exp.get_failed_trials()
run(failed_trials)

exp = Experiment(logdir, name, restore=True)
trials = exp.trials_finished()
trials.reset_status()
run(trials)


optimizer = Optimizer(sweep, metric, *parameters)
sweep.configure_server()
sweep.add_logger(Logging)
sweep.set_executor(executor)
sweep.run(func, verbose=verbose)