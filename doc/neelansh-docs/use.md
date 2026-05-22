# Usage Guide

This guide explains how to use the Ray clone repository for your personal projects and development.

## Basic Usage

To use Ray in your Python projects, you first need to initialize it:

```python
import ray

# Initialize Ray on your local machine
ray.init()

# Define a remote function (task)
@ray.remote
def add(x, y):
    return x + y

# Execute the task asynchronously
future = add.remote(1, 2)

# Get the result
print(ray.get(future))  # Output: 3
```

### Actors (Stateful)

Actors allow you to maintain state across multiple tasks:

```python
@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1
        return self.count

# Create an actor instance
counter = Counter.remote()

# Call methods on the actor
print(ray.get(counter.increment.remote()))  # Output: 1
print(ray.get(counter.increment.remote()))  # Output: 2
```

## Using Ray Libraries

Ray comes with several powerful libraries built on top of Ray Core:

- **Ray Data**: Scalable Datasets for ML.
- **Ray Train**: Distributed Training.
- **Ray Tune**: Scalable Hyperparameter Tuning.
- **RLlib**: Scalable Reinforcement Learning.
- **Ray Serve**: Scalable and Programmable Serving.

Example using Ray Serve:

```python
from ray import serve
import requests

@serve.deployment
def hello(request):
    return "Hello from Ray Serve!"

serve.run(hello.bind())
print(requests.get("http://localhost:8000/").text)
```

## Using the Ray Clone for Personal Projects

If you are developing features in this repository and want to use them in another project on your machine, you have two main options:

### 1. Editable Install (Recommended)

In your project's environment, navigate to the `python/` directory of this clone and install it in editable mode:

```bash
cd /path/to/ray-clone/python
pip install -e .
```

This way, any changes you make to the Python code in this clone will be immediately available in your other projects.

### 2. Building Wheels

You can build a wheel and install it elsewhere:

```bash
cd python
python setup.py bdist_wheel
pip install dist/ray-*.whl
```

## Monitoring and Debugging

Ray provides a dashboard to monitor your cluster and applications:

1. **Dashboard**: Accessible at `http://localhost:8265` by default when you run `ray.init()`.
2. **Logging**: Ray logs are stored in `/tmp/ray/session_latest/logs` by default.
3. **CLI**: Use the `ray` command-line tool for cluster management:
   ```bash
   ray status
   ray dashboard
   ```
