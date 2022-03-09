import pytest
import sys

import ray
from ray import serve
from ray.serve.application import Application
from ray._private.test_utils import wait_for_condition


class TestAddDeployment:
    @serve.deployment
    def f(*args):
        return "got f"

    @serve.deployment
    class C:
        def __call__(self, *args):
            return "got C"

    def test_add_deployment_valid(self):
        app = Application()
        app.add_deployment(self.f)
        app.add_deployment(self.C)

        assert len(app) == 2
        assert "f" in app
        assert "C" in app

    def test_add_deployment_repeat_name(self):
        with pytest.raises(ValueError):
            app = Application()
            app.add_deployment(self.f)
            app.add_deployment(self.C.options(name="f"))

        with pytest.raises(ValueError):
            Application([self.C, self.f.options(name="C")])


class TestDeploy2:
    @serve.deployment
    def f(*args):
        return "got f"

    @serve.deployment
    class C:
        def __call__(self, *args):
            return "got C"

    def test_deploy_valid(self, serve_instance):
        Application([self.f, self.C]).deploy()
        assert ray.get(serve.get_deployment("f").get_handle().remote()) == "got f"
        assert ray.get(serve.get_deployment("C").get_handle().remote()) == "got C"


class TestDeployGroup:
    @serve.deployment
    def f():
        return "f reached"

    @serve.deployment
    def g():
        return "g reached"

    @serve.deployment
    class C:
        async def __call__(self):
            return "C reached"

    @serve.deployment
    class D:
        async def __call__(self):
            return "D reached"

    def deploy_and_check_responses(
        self, deployments, responses, blocking=True, client=None
    ):
        """
        Helper function that deploys the list of deployments, calls them with
        their handles, and checks whether they return the objects in responses.
        If blocking is False, this function uses a non-blocking deploy and uses
        the client to wait until the deployments finish deploying.
        """

        Application(deployments).deploy(blocking=blocking)

        def check_all_deployed():
            try:
                for deployment, response in zip(deployments, responses):
                    if ray.get(deployment.get_handle().remote()) != response:
                        return False
            except Exception:
                return False

            return True

        if blocking:
            # If blocking, this should be guaranteed to pass immediately.
            assert check_all_deployed()
        else:
            # If non-blocking, this should pass eventually.
            wait_for_condition(check_all_deployed)

    def test_basic_deploy_group(self, serve_instance):
        """
        Atomically deploys a group of deployments, including both functions and
        classes. Checks whether they deploy correctly.
        """

        deployments = [self.f, self.g, self.C, self.D]
        responses = ["f reached", "g reached", "C reached", "D reached"]

        self.deploy_and_check_responses(deployments, responses)

    def test_non_blocking_deploy_group(self, serve_instance):
        """Checks Application's deploy() behavior when blocking=False."""

        deployments = [self.f, self.g, self.C, self.D]
        responses = ["f reached", "g reached", "C reached", "D reached"]
        self.deploy_and_check_responses(
            deployments, responses, blocking=False, client=serve_instance
        )

    def test_mutual_handles(self, serve_instance):
        """
        Atomically deploys a group of deployments that get handles to other
        deployments in the group inside their __init__ functions. The handle
        references should fail in a non-atomic deployment. Checks whether the
        deployments deploy correctly.
        """

        @serve.deployment
        class MutualHandles:
            async def __init__(self, handle_name):
                self.handle = serve.get_deployment(handle_name).get_handle()

            async def __call__(self, echo: str):
                return await self.handle.request_echo.remote(echo)

            async def request_echo(self, echo: str):
                return echo

        names = []
        for i in range(10):
            names.append("a" * i)

        deployments = []
        for idx in range(len(names)):
            # Each deployment will hold a ServeHandle with the next name in
            # the list
            deployment_name = names[idx]
            handle_name = names[(idx + 1) % len(names)]

            deployments.append(
                MutualHandles.options(name=deployment_name, init_args=(handle_name,))
            )

        Application(deployments).deploy(blocking=True)

        for deployment in deployments:
            assert (ray.get(deployment.get_handle().remote("hello"))) == "hello"

    def test_decorated_deployments(self, serve_instance):
        """
        Checks Application's deploy behavior when deployments have options set
        in their @serve.deployment decorator.
        """

        @serve.deployment(num_replicas=2, max_concurrent_queries=5)
        class DecoratedClass1:
            async def __call__(self):
                return "DecoratedClass1 reached"

        @serve.deployment(num_replicas=4, max_concurrent_queries=2)
        class DecoratedClass2:
            async def __call__(self):
                return "DecoratedClass2 reached"

        deployments = [DecoratedClass1, DecoratedClass2]
        responses = ["DecoratedClass1 reached", "DecoratedClass2 reached"]
        self.deploy_and_check_responses(deployments, responses)

    def test_empty_list(self, serve_instance):
        """Checks Application's deploy behavior when deployment group is empty."""

        self.deploy_and_check_responses([], [])

    def test_invalid_input(self, serve_instance):
        """
        Checks Application's deploy behavior when deployment group contains
        non-Deployment objects.
        """

        with pytest.raises(TypeError):
            Application([self.f, self.C, "not a Deployment object"]).deploy(
                blocking=True
            )

    def test_import_path_deployment(self, serve_instance):
        test_env_uri = (
            "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
        )
        test_module_uri = (
            "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
        )

        ray_actor_options = {
            "runtime_env": {"py_modules": [test_env_uri, test_module_uri]}
        }

        shallow = serve.deployment(
            name="shallow",
            ray_actor_options=ray_actor_options,
        )("test_env.shallow_import.ShallowClass")

        deep = serve.deployment(
            name="deep",
            ray_actor_options=ray_actor_options,
        )("test_env.subdir1.subdir2.deep_import.DeepClass")

        one = serve.deployment(
            name="one",
            ray_actor_options=ray_actor_options,
        )("test_module.test.one")

        deployments = [shallow, deep, one]
        responses = ["Hello shallow world!", "Hello deep world!", 2]

        self.deploy_and_check_responses(deployments, responses)

    def test_different_pymodules(self, serve_instance):
        test_env_uri = (
            "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
        )
        test_module_uri = (
            "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
        )

        shallow = serve.deployment(
            name="shallow",
            ray_actor_options={"runtime_env": {"py_modules": [test_env_uri]}},
        )("test_env.shallow_import.ShallowClass")

        one = serve.deployment(
            name="one",
            ray_actor_options={"runtime_env": {"py_modules": [test_module_uri]}},
        )("test_module.test.one")

        deployments = [shallow, one]
        responses = ["Hello shallow world!", 2]

        self.deploy_and_check_responses(deployments, responses)

    def test_import_path_deployment_decorated(self, serve_instance):
        func = serve.deployment(
            name="decorated_func",
        )("ray.serve.tests.test_deploy.decorated_func")

        clss = serve.deployment(
            name="decorated_clss",
        )("ray.serve.tests.test_deploy.DecoratedClass")

        deployments = [func, clss]
        responses = ["got decorated func", "got decorated class"]

        self.deploy_and_check_responses(deployments, responses)

        # Check that non-default decorated values were overwritten
        assert serve.get_deployment("decorated_func").max_concurrent_queries != 17
        assert serve.get_deployment("decorated_clss").max_concurrent_queries != 17


# Decorated function with non-default max_concurrent queries
@serve.deployment(max_concurrent_queries=17)
def decorated_func(req=None):
    return "got decorated func"


# Decorated class with non-default max_concurrent queries
@serve.deployment(max_concurrent_queries=17)
class DecoratedClass:
    def __call__(self, req=None):
        return "got decorated class"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
