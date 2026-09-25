"""Tests for Anthropic ingress route registration."""

import sys

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute

from ray.llm._internal.serve.core.ingress.ingress import (
    DEFAULT_ANTHROPIC_ENDPOINTS,
    AnthropicIngress,
    make_fastapi_ingress,
)


class TestAnthropicIngressRoutes:
    def test_anthropic_ingress_registers_messages_route(self):
        app = FastAPI()
        make_fastapi_ingress(
            AnthropicIngress, endpoint_map=DEFAULT_ANTHROPIC_ENDPOINTS, app=app
        )
        route_paths = [
            route.path for route in app.routes if isinstance(route, APIRoute)
        ]
        assert "/v1/messages" in route_paths

    def test_anthropic_ingress_registers_count_tokens_route(self):
        app = FastAPI()
        make_fastapi_ingress(
            AnthropicIngress, endpoint_map=DEFAULT_ANTHROPIC_ENDPOINTS, app=app
        )
        route_paths = [
            route.path for route in app.routes if isinstance(route, APIRoute)
        ]
        assert "/v1/messages/count_tokens" in route_paths


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
