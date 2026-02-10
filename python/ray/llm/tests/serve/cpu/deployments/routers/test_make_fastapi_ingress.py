"""Tests for make_fastapi_ingress function."""

import inspect
import sys

import pytest
from fastapi import FastAPI, Request
from fastapi.routing import APIRoute

from ray.llm._internal.serve.core.ingress.ingress import (
    DEFAULT_ENDPOINTS,
    OpenAiIngress,
    make_fastapi_ingress,
)


class TestMakeFastapiIngress:
    """Test suite for make_fastapi_ingress."""

    def test_subclass_inherits_endpoints(self):
        """Test that subclassing OpenAiIngress works with make_fastapi_ingress."""

        class MyCustomIngress(OpenAiIngress):
            """Custom ingress that inherits all OpenAI endpoints."""

            pass

        app = FastAPI()
        # Create the ingress class - should not raise
        ingress_cls = make_fastapi_ingress(MyCustomIngress, app=app)

        # Verify the ingress class was created successfully
        assert ingress_cls is not None

        # Verify routes are registered (inherited from OpenAiIngress)
        route_paths = [
            route.path for route in app.routes if isinstance(route, APIRoute)
        ]
        assert "/v1/models" in route_paths
        assert "/v1/completions" in route_paths

    def test_subclass_with_custom_method(self):
        """Test that custom methods added by subclass are also properly handled."""

        class MyCustomIngress(OpenAiIngress):
            """Custom ingress with an additional endpoint."""

            async def custom_endpoint(self, request: Request):
                """A custom endpoint added by the subclass."""
                return {"status": "ok"}

        custom_endpoints = {
            "custom_endpoint": lambda app: app.post("/custom"),
            **DEFAULT_ENDPOINTS,
        }

        app = FastAPI()
        ingress_cls = make_fastapi_ingress(
            MyCustomIngress, endpoint_map=custom_endpoints, app=app
        )

        # Verify the class was created and the custom route is registered
        assert ingress_cls is not None
        route_paths = [
            route.path for route in app.routes if isinstance(route, APIRoute)
        ]
        assert "/custom" in route_paths

    def test_routes_registered_correctly(self):
        """Test that routes are registered with the FastAPI app."""

        class MyCustomIngress(OpenAiIngress):
            pass

        app = FastAPI()
        make_fastapi_ingress(MyCustomIngress, app=app)

        # Get all registered routes
        route_paths = [
            route.path for route in app.routes if isinstance(route, APIRoute)
        ]

        # Check that default endpoints are registered
        assert "/v1/models" in route_paths
        assert "/v1/completions" in route_paths
        assert "/v1/chat/completions" in route_paths

    def test_custom_endpoint_map_overrides_defaults(self):
        """Test that custom endpoint_map can override default endpoints."""

        class MyCustomIngress(OpenAiIngress):
            async def models(self):
                """Override the models endpoint."""
                return {"custom": True}

        # Only register models endpoint with a custom path
        custom_endpoints = {
            "models": lambda app: app.get("/custom/models"),
        }

        app = FastAPI()
        make_fastapi_ingress(MyCustomIngress, endpoint_map=custom_endpoints, app=app)

        route_paths = [
            route.path for route in app.routes if isinstance(route, APIRoute)
        ]

        # Should have custom path, not default
        assert "/custom/models" in route_paths
        assert "/v1/models" not in route_paths

    def test_deeply_nested_inheritance(self):
        """Test that deeply nested inheritance works correctly."""

        class IntermediateIngress(OpenAiIngress):
            """Intermediate class in inheritance chain."""

            async def intermediate_method(self, request: Request):
                return {"level": "intermediate"}

        class FinalIngress(IntermediateIngress):
            """Final class in inheritance chain."""

            async def final_method(self, request: Request):
                return {"level": "final"}

        custom_endpoints = {
            "intermediate_method": lambda app: app.post("/intermediate"),
            "final_method": lambda app: app.post("/final"),
            **DEFAULT_ENDPOINTS,
        }

        app = FastAPI()
        make_fastapi_ingress(FinalIngress, endpoint_map=custom_endpoints, app=app)

        # Verify all routes are registered
        route_paths = [
            route.path for route in app.routes if isinstance(route, APIRoute)
        ]
        assert "/intermediate" in route_paths
        assert "/final" in route_paths
        assert "/v1/completions" in route_paths

    def test_method_signature_preserved(self):
        """Test that method signatures are preserved after decoration."""

        class MyCustomIngress(OpenAiIngress):
            pass

        ingress_cls = make_fastapi_ingress(MyCustomIngress)

        # Get the completions method and check its signature
        completions_method = ingress_cls.completions
        sig = inspect.signature(completions_method)
        param_names = list(sig.parameters.keys())

        # Should have 'self' and 'body' parameters
        assert "self" in param_names
        assert "body" in param_names


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
