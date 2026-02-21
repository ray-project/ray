import asyncio
import sys

import pytest
import requests
from fastapi import FastAPI

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve.api import get_deployment_handle
from ray.serve.context import _get_global_client
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import ApplicationStatus


class TestDeploymentTopology:
    def test_simple_chain_topology(self, serve_instance):
        """Test topology with a simple chain: ingress -> downstream."""

        # Define a downstream deployment
        @serve.deployment
        class Downstream:
            def __call__(self):
                return "downstream response"

        # Define an ingress deployment that calls downstream
        @serve.deployment
        class Ingress:
            def __init__(self, downstream: DeploymentHandle):
                self.downstream = downstream

            async def __call__(self):
                result = await self.downstream.remote()
                return f"ingress -> {result}"

        # Deploy the application
        downstream = Downstream.bind()
        ingress = Ingress.bind(downstream)
        serve.run(ingress, name="test_app", route_prefix="/test")

        # Make a request to ensure deployments are fully initialized
        response = requests.get("http://localhost:8000/test")
        assert response.status_code == 200
        assert "downstream response" in response.text

        # Define expected topology
        expected_topology = {
            "app_name": "test_app",
            "ingress_deployment": "Ingress",
            "nodes": {
                "Ingress": {
                    "name": "Ingress",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [
                        {"name": "Downstream", "app_name": "test_app"}
                    ],
                },
                "Downstream": {
                    "name": "Downstream",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
            },
        }

        # Final verification - get the actual topology
        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology = status["applications"]["test_app"]["deployment_topology"]

        # Direct comparison with expected topology
        assert actual_topology == expected_topology

    def test_multi_level_chain_topology(self, serve_instance):
        """Test topology with deep dependencies: Ingress -> A -> B -> C."""

        @serve.deployment
        class ServiceC:
            def __call__(self):
                return "C"

        @serve.deployment
        class ServiceB:
            def __init__(self, service_c: DeploymentHandle):
                self.service_c = service_c

            async def __call__(self):
                result = await self.service_c.remote()
                return f"B->{result}"

        @serve.deployment
        class ServiceA:
            def __init__(self, service_b: DeploymentHandle):
                self.service_b = service_b

            async def __call__(self):
                result = await self.service_b.remote()
                return f"A->{result}"

        @serve.deployment
        class Ingress:
            def __init__(self, service_a: DeploymentHandle):
                self.service_a = service_a

            async def __call__(self):
                result = await self.service_a.remote()
                return f"Ingress->{result}"

        # Deploy the application
        service_c = ServiceC.bind()
        service_b = ServiceB.bind(service_c)
        service_a = ServiceA.bind(service_b)
        ingress = Ingress.bind(service_a)
        serve.run(ingress, name="test_app", route_prefix="/test")

        # Make a request
        response = requests.get("http://localhost:8000/test")
        assert response.status_code == 200
        assert "C" in response.text

        # Expected topology
        expected_topology = {
            "app_name": "test_app",
            "ingress_deployment": "Ingress",
            "nodes": {
                "Ingress": {
                    "name": "Ingress",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [
                        {"name": "ServiceA", "app_name": "test_app"}
                    ],
                },
                "ServiceA": {
                    "name": "ServiceA",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [
                        {"name": "ServiceB", "app_name": "test_app"}
                    ],
                },
                "ServiceB": {
                    "name": "ServiceB",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [
                        {"name": "ServiceC", "app_name": "test_app"}
                    ],
                },
                "ServiceC": {
                    "name": "ServiceC",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
            },
        }

        # Final verification
        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology = status["applications"]["test_app"]["deployment_topology"]
        assert actual_topology == expected_topology

    def test_fan_out_topology(self, serve_instance):
        """Test topology with fan-out: Ingress -> [Service1, Service2, Service3]."""

        @serve.deployment
        class Service1:
            def __call__(self):
                return "service1"

        @serve.deployment
        class Service2:
            def __call__(self):
                return "service2"

        @serve.deployment
        class Service3:
            def __call__(self):
                return "service3"

        @serve.deployment
        class Ingress:
            def __init__(
                self,
                service1: DeploymentHandle,
                service2: DeploymentHandle,
                service3: DeploymentHandle,
            ):
                self.service1 = service1
                self.service2 = service2
                self.service3 = service3

            async def __call__(self):
                results = await asyncio.gather(
                    self.service1.remote(),
                    self.service2.remote(),
                    self.service3.remote(),
                )
                return f"Results: {','.join(results)}"

        # Deploy the application
        service1 = Service1.bind()
        service2 = Service2.bind()
        service3 = Service3.bind()
        ingress = Ingress.bind(service1, service2, service3)
        serve.run(ingress, name="test_app", route_prefix="/test")

        # Make a request
        response = requests.get("http://localhost:8000/test")
        assert response.status_code == 200

        # Expected topology - outbound_deployments should contain all three services
        expected_topology = {
            "app_name": "test_app",
            "ingress_deployment": "Ingress",
            "nodes": {
                "Ingress": {
                    "name": "Ingress",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [
                        {"name": "Service1", "app_name": "test_app"},
                        {"name": "Service2", "app_name": "test_app"},
                        {"name": "Service3", "app_name": "test_app"},
                    ],
                },
                "Service1": {
                    "name": "Service1",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
                "Service2": {
                    "name": "Service2",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
                "Service3": {
                    "name": "Service3",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
            },
        }

        # Final verification
        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology = status["applications"]["test_app"]["deployment_topology"]
        # Sort for comparison
        actual_topology["nodes"]["Ingress"]["outbound_deployments"] = sorted(
            actual_topology["nodes"]["Ingress"]["outbound_deployments"],
            key=lambda x: x["name"],
        )
        expected_topology["nodes"]["Ingress"]["outbound_deployments"] = sorted(
            expected_topology["nodes"]["Ingress"]["outbound_deployments"],
            key=lambda x: x["name"],
        )
        assert actual_topology == expected_topology

    def test_diamond_topology(self, serve_instance):
        """Test diamond pattern: Ingress -> [ServiceA, ServiceB] -> Database."""

        @serve.deployment
        class Database:
            def __call__(self):
                return "db_data"

        @serve.deployment
        class ServiceA:
            def __init__(self, database: DeploymentHandle):
                self.database = database

            async def __call__(self):
                data = await self.database.remote()
                return f"A:{data}"

        @serve.deployment
        class ServiceB:
            def __init__(self, database: DeploymentHandle):
                self.database = database

            async def __call__(self):
                data = await self.database.remote()
                return f"B:{data}"

        @serve.deployment
        class Ingress:
            def __init__(
                self, service_a: DeploymentHandle, service_b: DeploymentHandle
            ):
                self.service_a = service_a
                self.service_b = service_b

            async def __call__(self):
                results = await asyncio.gather(
                    self.service_a.remote(),
                    self.service_b.remote(),
                )
                return f"Results: {','.join(results)}"

        # Deploy the application
        database = Database.bind()
        service_a = ServiceA.bind(database)
        service_b = ServiceB.bind(database)
        ingress = Ingress.bind(service_a, service_b)
        serve.run(ingress, name="test_app", route_prefix="/test")

        # Make a request
        response = requests.get("http://localhost:8000/test")
        assert response.status_code == 200

        # Expected topology
        expected_topology = {
            "app_name": "test_app",
            "ingress_deployment": "Ingress",
            "nodes": {
                "Ingress": {
                    "name": "Ingress",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [
                        {"name": "ServiceA", "app_name": "test_app"},
                        {"name": "ServiceB", "app_name": "test_app"},
                    ],
                },
                "ServiceA": {
                    "name": "ServiceA",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [
                        {"name": "Database", "app_name": "test_app"}
                    ],
                },
                "ServiceB": {
                    "name": "ServiceB",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [
                        {"name": "Database", "app_name": "test_app"}
                    ],
                },
                "Database": {
                    "name": "Database",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
            },
        }

        # Final verification
        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology = status["applications"]["test_app"]["deployment_topology"]
        # Sort for comparison
        actual_topology["nodes"]["Ingress"]["outbound_deployments"] = sorted(
            actual_topology["nodes"]["Ingress"]["outbound_deployments"],
            key=lambda x: x["name"],
        )
        expected_topology["nodes"]["Ingress"]["outbound_deployments"] = sorted(
            expected_topology["nodes"]["Ingress"]["outbound_deployments"],
            key=lambda x: x["name"],
        )
        assert actual_topology == expected_topology

    def test_cross_app_topology(self, serve_instance):
        """Test cross-application dependencies."""

        # Deploy App2 first (the dependency)
        @serve.deployment
        class App2Service:
            def __call__(self):
                return "app2_response"

        app2_service = App2Service.bind()
        serve.run(app2_service, name="app2", route_prefix="/app2")

        # Deploy App1 that depends on App2
        @serve.deployment
        class App1Service:
            def __init__(self):
                self.app2_handle = get_deployment_handle("App2Service", "app2")

            async def __call__(self):
                result = await self.app2_handle.remote()
                return f"app1->{result}"

        app1_service = App1Service.bind()
        serve.run(app1_service, name="app1", route_prefix="/app1")

        # Make requests to both apps to ensure they're initialized
        response1 = requests.get("http://localhost:8000/app1")
        assert response1.status_code == 200
        response2 = requests.get("http://localhost:8000/app2")
        assert response2.status_code == 200

        # Expected topology for app1
        expected_topology_app1 = {
            "app_name": "app1",
            "ingress_deployment": "App1Service",
            "nodes": {
                "App1Service": {
                    "name": "App1Service",
                    "app_name": "app1",
                    "is_ingress": True,
                    "outbound_deployments": [
                        {"name": "App2Service", "app_name": "app2"}
                    ],
                },
            },
        }

        # Expected topology for app2
        expected_topology_app2 = {
            "app_name": "app2",
            "ingress_deployment": "App2Service",
            "nodes": {
                "App2Service": {
                    "name": "App2Service",
                    "app_name": "app2",
                    "is_ingress": True,
                    "outbound_deployments": [],
                },
            },
        }

        # Wait for topologies to be built
        def check_topology():
            client = _get_global_client()
            status = client.get_serve_details()
            topology_app1 = (
                status.get("applications", {})
                .get("app1", {})
                .get("deployment_topology")
            )
            topology_app2 = (
                status.get("applications", {})
                .get("app2", {})
                .get("deployment_topology")
            )
            return (
                topology_app1 == expected_topology_app1
                and topology_app2 == expected_topology_app2
            )

        # NOTE(abrar): using wait_for_condition because outbound deployments are added asynchronously
        # when get_deployment_handle is used.
        wait_for_condition(check_topology)

        # Final verification
        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology_app1 = status["applications"]["app1"]["deployment_topology"]
        actual_topology_app2 = status["applications"]["app2"]["deployment_topology"]
        assert actual_topology_app1 == expected_topology_app1
        assert actual_topology_app2 == expected_topology_app2

    def test_single_deployment_no_dependencies(self, serve_instance):
        """Test single deployment with no outbound dependencies."""

        @serve.deployment
        class Standalone:
            def __call__(self):
                return "standalone"

        standalone = Standalone.bind()
        serve.run(standalone, name="test_app", route_prefix="/test")

        # Make a request
        response = requests.get("http://localhost:8000/test")
        assert response.status_code == 200
        assert response.text == "standalone"

        # Expected topology
        expected_topology = {
            "app_name": "test_app",
            "ingress_deployment": "Standalone",
            "nodes": {
                "Standalone": {
                    "name": "Standalone",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [],
                },
            },
        }

        # Final verification
        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology = status["applications"]["test_app"]["deployment_topology"]
        assert actual_topology == expected_topology

    def test_topology_update_on_redeploy(self, serve_instance):
        """Test that topology updates when application is redeployed with different structure."""

        # Initial deployment: Ingress -> ServiceA
        @serve.deployment
        class ServiceA:
            def __call__(self):
                return "A"

        @serve.deployment
        class Ingress:
            def __init__(self, service_a: DeploymentHandle):
                self.service_a = service_a

            async def __call__(self):
                result = await self.service_a.remote()
                return f"Ingress->{result}"

        service_a = ServiceA.bind()
        ingress = Ingress.bind(service_a)
        serve.run(ingress, name="test_app", route_prefix="/test")

        # Make a request
        response = requests.get("http://localhost:8000/test")
        assert response.status_code == 200

        # Expected initial topology
        expected_topology_v1 = {
            "app_name": "test_app",
            "ingress_deployment": "Ingress",
            "nodes": {
                "Ingress": {
                    "name": "Ingress",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [
                        {"name": "ServiceA", "app_name": "test_app"}
                    ],
                },
                "ServiceA": {
                    "name": "ServiceA",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
            },
        }
        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology = status["applications"]["test_app"]["deployment_topology"]
        assert expected_topology_v1 == actual_topology

        # Redeploy with different structure: Ingress -> ServiceB
        @serve.deployment
        class ServiceB:
            def __call__(self):
                return "B"

        @serve.deployment
        class IngressV2:
            def __init__(self, service_b: DeploymentHandle):
                self.service_b = service_b

            async def __call__(self):
                result = await self.service_b.remote()
                return f"IngressV2->{result}"

        service_b = ServiceB.bind()
        ingress_v2 = IngressV2.bind(service_b)
        serve.run(ingress_v2, name="test_app", route_prefix="/test")

        # Make a request to new deployment
        response = requests.get("http://localhost:8000/test")
        assert response.status_code == 200
        assert "B" in response.text

        # Expected updated topology
        expected_topology_v2 = {
            "app_name": "test_app",
            "ingress_deployment": "IngressV2",
            "nodes": {
                "IngressV2": {
                    "name": "IngressV2",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [
                        {"name": "ServiceB", "app_name": "test_app"}
                    ],
                },
                "ServiceB": {
                    "name": "ServiceB",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
            },
        }

        # Final verification
        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology = status["applications"]["test_app"]["deployment_topology"]
        assert actual_topology == expected_topology_v2

    def test_fastapi_factory_with_get_deployment_handle(self, serve_instance):
        """Test topology with FastAPI factory pattern using get_deployment_handle.

        This test demonstrates the pattern where get_deployment_handle is used
        inside a FastAPI request handler (lazy lookup), which is common in
        factory patterns where dependencies aren't injected at construction time.
        """

        # Backend service that will be called via get_deployment_handle
        @serve.deployment(name="BackendService")
        class BackendService:
            def __call__(self):
                return "backend_data"

        # FastAPI ingress using factory pattern
        def create_fastapi_app():
            app = FastAPI()

            @app.get("/")
            async def root():
                # Use get_deployment_handle inside the request handler (lazy lookup)
                # This is the factory pattern - handle is obtained at request time
                backend_handle = get_deployment_handle("BackendService", "test_app")
                result = await backend_handle.remote()
                return {"data": result}

            @app.get("/health")
            async def health():
                return {"status": "ok"}

            return app

        @serve.deployment(name="FastAPIIngress")
        @serve.ingress(create_fastapi_app())
        class FastAPIIngress:
            def __init__(self, backend_service: DeploymentHandle):
                self.backend_service = backend_service

        # Deploy both deployments
        ingress_deployment = FastAPIIngress.bind(BackendService.bind())

        serve.run(ingress_deployment, name="test_app", route_prefix="/test")

        # Make requests to ensure deployments are initialized
        response = requests.get("http://localhost:8000/test/health")
        assert response.status_code == 200

        # Make a request that triggers get_deployment_handle
        response = requests.get("http://localhost:8000/test/")
        assert response.status_code == 200
        assert "backend_data" in response.text

        # Expected topology
        expected_topology = {
            "app_name": "test_app",
            "ingress_deployment": "FastAPIIngress",
            "nodes": {
                "FastAPIIngress": {
                    "name": "FastAPIIngress",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [
                        {"name": "BackendService", "app_name": "test_app"}
                    ],
                },
                "BackendService": {
                    "name": "BackendService",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
            },
        }

        # Wait for topology to be built
        # NOTE(abrar): using wait_for_condition because outbound deployments are added asynchronously
        # when get_deployment_handle is used.
        def check_topology():
            client = _get_global_client()
            status = client.get_serve_details()
            topology = (
                status.get("applications", {})
                .get("test_app", {})
                .get("deployment_topology")
            )
            return topology == expected_topology

        wait_for_condition(check_topology)

        # Final verification
        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology = status["applications"]["test_app"]["deployment_topology"]
        assert actual_topology == expected_topology

    def test_topology_with_updating_deployment(self, serve_instance):
        """Test topology when a deployment is updating (not fully rolled out)."""

        # Create signal actor for synchronization
        signal = SignalActor.remote()

        # Initial deployment
        @serve.deployment(name="ServiceA", version="v1")
        class ServiceA:
            def __call__(self):
                return "v1"

        @serve.deployment(name="Ingress")
        class Ingress:
            def __init__(self, service_a: DeploymentHandle):
                # Block until signal is sent to keep deployment in UPDATING state
                ray.get(signal.wait.remote())
                self.service_a = service_a

            async def __call__(self):
                result = await self.service_a.remote()
                return f"Ingress->{result}"

        # Deploy initial version
        service_a_v1 = ServiceA.bind()
        ingress = Ingress.bind(service_a_v1)
        serve._run(ingress, name="test_app", route_prefix="/test", _blocking=False)

        wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 1)

        # Verify initial topology
        initial_topology = {
            "app_name": "test_app",
            "ingress_deployment": "Ingress",
            "nodes": {
                "Ingress": {
                    "name": "Ingress",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [],
                },
                "ServiceA": {
                    "name": "ServiceA",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
            },
        }

        def check_initial_topology():
            client = _get_global_client()
            status = client.get_serve_details()
            if "test_app" not in status["applications"]:
                return False
            app_status = status["applications"]["test_app"]["status"]
            return app_status == ApplicationStatus.DEPLOYING.value

        wait_for_condition(check_initial_topology)

        client = _get_global_client()
        status = client.get_serve_details()
        actual_topology = status["applications"]["test_app"]["deployment_topology"]
        assert actual_topology == initial_topology

        # Clean up: send signal to unblock replicas
        ray.get(signal.send.remote())

    def test_topology_with_failed_deployment(self, serve_instance):
        """Test topology when a deployment fails to start."""

        # A deployment that will fail to start
        @serve.deployment
        class ServiceB:
            def __call__(self):
                return "b"

        @serve.deployment(name="FailingService")
        class FailingService:
            def __init__(self, service_b: DeploymentHandle):
                self.service_b = service_b
                raise RuntimeError("Intentional failure for testing")

            def __call__(self):
                return "should never reach here"

        @serve.deployment(name="Ingress")
        class Ingress:
            def __init__(self, failing_service: DeploymentHandle):
                self.failing_service = failing_service

            async def __call__(self):
                result = await self.failing_service.remote()
                return f"Ingress->{result}"

        # Deploy with failing service
        service_b = ServiceB.bind()
        failing_service = FailingService.bind(service_b)
        ingress = Ingress.bind(failing_service)

        # Deploy (this won't fail immediately, but replicas will fail to start)
        serve._run(ingress, name="test_app", route_prefix="/test", _blocking=False)

        # Wait for the deployment to be processed and replicas to fail
        def deployment_has_failures():
            client = _get_global_client()
            status = client.get_serve_details()
            if "test_app" not in status["applications"]:
                return False
            app_status = status["applications"]["test_app"]["status"]
            return app_status == ApplicationStatus.DEPLOY_FAILED.value

        wait_for_condition(deployment_has_failures, timeout=10)

        # Get the serve details
        client = _get_global_client()
        status = client.get_serve_details()

        # Verify the application exists (even with failed deployment)
        assert "test_app" in status["applications"]

        # Verify deployment_topology still exists
        # The topology should be built based on the deployment structure,
        # regardless of whether the replicas are healthy
        topology = status["applications"]["test_app"]["deployment_topology"]
        expected_topology = {
            "app_name": "test_app",
            "ingress_deployment": "Ingress",
            "nodes": {
                "Ingress": {
                    "name": "Ingress",
                    "app_name": "test_app",
                    "is_ingress": True,
                    "outbound_deployments": [
                        {"name": "FailingService", "app_name": "test_app"}
                    ],
                },
                "FailingService": {
                    "name": "FailingService",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
                "ServiceB": {
                    "name": "ServiceB",
                    "app_name": "test_app",
                    "is_ingress": False,
                    "outbound_deployments": [],
                },
            },
        }
        assert topology == expected_topology


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
