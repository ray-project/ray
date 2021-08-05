Environment is described in runtime_env.yaml.

Supported commands:
- anyscale run driver.py # Run the command on my local machine in the local env. Env will be cached across runs.
- anyscale shell # Open a shell running in the local env. This could be used to e.g., open IPython and do some dev.
- anyscale exec "python driver.py" # Run an arbitrary shell command in the local env. This could be how we implement job submission.
