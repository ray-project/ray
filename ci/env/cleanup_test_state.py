"""
This script is used to clean up state after running test scripts, including
on external services. For instance, this script can be used to remove the runs
from WandB that have been saved during unit testing or when running examples.
"""
import sys


def clear_wandb_project():
    import wandb

    # This is hardcoded in the `ray/ml/examples/upload_to_wandb.py` example
    wandb_project = "ray_air_example"

    api = wandb.Api()
    for run in api.runs(wandb_project):
        run.delete()


def clear_comet_ml_project():
    import comet_ml

    # This is hardcoded in the `ray/ml/examples/upload_to_comet_ml.py` example
    comet_ml_project = "ray-air-example"

    api = comet_ml.API()
    workspace = api.get_default_workspace()
    experiments = api.get_experiments(
        workspace=workspace, project_name=comet_ml_project
    )
    api.delete_experiments([experiment.key for experiment in experiments])


SERVICES = {"wandb": clear_wandb_project, "comet_ml": clear_comet_ml_project}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <service1> [service2] ...")
        sys.exit(0)

    services = sys.argv[1:]

    if any(service not in SERVICES for service in services):
        raise RuntimeError(
            f"All services must be included in {list(SERVICES.keys())}. "
            f"Got: {services}"
        )

    for service in services:
        SERVICES[service]()
