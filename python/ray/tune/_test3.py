checkpoint_manager = State(location)
checkpoint_manager.optimizer_state
checkpoint_manager.generator_state
checkpoint_manager.trial_state

# How much have we learned
optimizer = Optimizer.from_checkpoint(checkpoint_manager)

optimizer = Optimizer(space, checkpoint=checkpoint_manager)
for x, y in warm_start:
	optimizer.report(x, y)

samples = [optimizer.sample(random=True) for i in range(50)]

spec = TrialSpec(func, local_dir, checkpoint)

generator = TrialGenerator.from_checkpoint(checkpoint, optimizer)

generator = TrialGenerator.from_trials(trials)

generator = TrialGenerator.from_spec(spec, optimizer)
generator.configure(checkpoint_callback)
generator.queue(samples)
generator.queue(num_samples=50, repeat=3, max_concurrent=4)
generator.next()

generator = TrialGenerator.from_multi_spec(spec)

run(generator)
###################################################

# Exploration process
trial_list = get_trials(checkpoint_manager)
failed_trials = [t.reset() for t in trial_list if t.status == "FAILED"]

generator = TrialGenerator.from_trials(failed_trials)
tune.run(generator)

builder = Builder()

for params in samples:
	yield builder.build(params)