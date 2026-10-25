# Run in a subprocess by test_get_serve_instance_details_for_imperative_apps

from ray import serve
from ray.serve.tests.test_config_files import world


def submit_imperative_apps() -> None:
    serve.run(world.DagNode, name="app1", route_prefix="/apple")
    print("Deployed app1")

    serve.run(world.DagNode, name="app2", route_prefix="/banana")
    print("Deployed app2")


if __name__ == "__main__":
    submit_imperative_apps()
