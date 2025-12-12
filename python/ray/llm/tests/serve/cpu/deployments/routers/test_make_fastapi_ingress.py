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


class TestMakeFastapiIngressInheritance:
    """Test suite for make_fastapi_ingress with inherited classes.

    These tests verify that when subclassing OpenAiIngress, inherited methods
    are properly transformed so that `self` is treated as a dependency
    (instance binding) rather than a query parameter.
    """

    def test_subclass_methods_have_correct_qualname(self):
        """Test that inherited methods get their __qualname__ updated to the subclass.

        If inherited methods keep their parent's __qualname__, they won't be
        recognized as belonging to the subclass and won't be transformed.
        """

        class MyCustomIngress(OpenAiIngress):
            """Custom ingress that inherits all OpenAI endpoints."""

            pass

        # Create the ingress class
        ingress_cls = make_fastapi_ingress(MyCustomIngress)

        # Check that the class qualname matches the input class qualname
        # (classes defined inside methods have full path in qualname)
        assert ingress_cls.__qualname__ == MyCustomIngress.__qualname__

        # Check that inherited methods have updated __qualname__
        for method_name in DEFAULT_ENDPOINTS.keys():
            method = getattr(ingress_cls, method_name)
            assert "MyCustomIngress" in method.__qualname__, (
                f"Method {method_name} should have MyCustomIngress in __qualname__, "
                f"but got {method.__qualname__}"
            )

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

        ingress_cls = make_fastapi_ingress(
            MyCustomIngress, endpoint_map=custom_endpoints
        )

        # Check custom method has correct qualname
        custom_method = ingress_cls.custom_endpoint
        assert "MyCustomIngress" in custom_method.__qualname__

        # Check inherited methods also have correct qualname
        completions_method = ingress_cls.completions
        assert "MyCustomIngress" in completions_method.__qualname__

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
        """Test that deeply nested inheritance also works correctly."""

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

        ingress_cls = make_fastapi_ingress(FinalIngress, endpoint_map=custom_endpoints)

        # All methods should have FinalIngress in their qualname
        for method_name in custom_endpoints.keys():
            method = getattr(ingress_cls, method_name)
            assert "FinalIngress" in method.__qualname__, (
                f"Method {method_name} should have FinalIngress in __qualname__, "
                f"but got {method.__qualname__}"
            )

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
