import sys

import pytest
from pydantic import ValidationError

from ray.serve.config import HTTPOptions
from ray.serve.schema import HTTPOptionsSchema, ServeDeploySchema


@pytest.mark.parametrize("options_type", [HTTPOptions, HTTPOptionsSchema])
@pytest.mark.parametrize(
    "tls_options",
    [
        {"ssl_keyfile": "key.pem"},
        {"ssl_keyfile": "key.pem", "ssl_certfile": None},
        {"ssl_certfile": "cert.pem"},
        {"ssl_certfile": "cert.pem", "ssl_keyfile": None},
    ],
)
def test_http_options_reject_incomplete_tls(options_type, tls_options):
    with pytest.raises(ValidationError, match="Both ssl_keyfile and ssl_certfile"):
        options_type(**tls_options)


@pytest.mark.parametrize("options_type", [HTTPOptions, HTTPOptionsSchema])
@pytest.mark.parametrize(
    "tls_options",
    [
        {},
        {"ssl_keyfile": None},
        {"ssl_keyfile": None, "ssl_certfile": None},
        {"ssl_keyfile": "key.pem", "ssl_certfile": "cert.pem"},
    ],
)
def test_http_options_accept_complete_or_disabled_tls(options_type, tls_options):
    options = options_type(**tls_options)
    assert options.ssl_keyfile == tls_options.get("ssl_keyfile")
    assert options.ssl_certfile == tls_options.get("ssl_certfile")
    assert options.model_fields_set == set(tls_options)


def test_serve_deploy_schema_rejects_incomplete_tls():
    with pytest.raises(ValidationError, match="Both ssl_keyfile and ssl_certfile"):
        ServeDeploySchema(applications=[], http_options={"ssl_keyfile": "key.pem"})


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
