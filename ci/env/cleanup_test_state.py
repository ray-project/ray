import sys


def clear_wandb_project():
    import wandb

    # This is hardcoded in the `ray/ml/examples/upload_to_wandb.py` example
    wandb_project = "ray_air_example"

    api = wandb.Api()
    for run in api.runs(wandb_project):
        run.delete()


SERVICES = {"wandb": clear_wandb_project}


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
