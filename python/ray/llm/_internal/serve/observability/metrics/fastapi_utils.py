"""This file contains constants and utility functions for FastAPI."""

from fastapi import FastAPI

# These tag keys are used in metrics for the FastAPI app.
FASTAPI_HTTP_RESPONSE_CODE_TAG_KEY = "code"
FASTAPI_HTTP_HANDLER_TAG_KEY = "handler"
FASTAPI_HTTP_METHOD_TAG_KEY = "method"
FASTAPI_HTTP_PATH_TAG_KEY = "path"
FASTAPI_HTTP_USER_ID_TAG_KEY = "user_id"
FASTAPI_API_SERVER_TAG_KEY = "api_server"

FASTAPI_BASE_HTTP_METRIC_TAG_KEYS = (
    FASTAPI_HTTP_RESPONSE_CODE_TAG_KEY,
    FASTAPI_HTTP_HANDLER_TAG_KEY,
    FASTAPI_HTTP_METHOD_TAG_KEY,
    FASTAPI_HTTP_PATH_TAG_KEY,
    FASTAPI_HTTP_USER_ID_TAG_KEY,
    FASTAPI_API_SERVER_TAG_KEY,
)


def get_app_name(app: FastAPI) -> str:
    """Gets the FastAPI app name."""

    return getattr(app.state, "name", "unknown")
