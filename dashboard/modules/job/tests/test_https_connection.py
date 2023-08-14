import pytest
import ssl
import sys
import trustme

import ray
from ray.job_submission import JobSubmissionClient


@pytest.fixture(scope="session")
def ca():
    return trustme.CA()


@pytest.fixture(scope="session")
def httpserver_ssl_context(ca):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    localhost_cert = ca.issue_cert("localhost")
    localhost_cert.configure_cert(context)
    return context


@pytest.fixture(scope="session")
def httpclient_ssl_context(ca):
    with ca.cert_pem.tempfile() as ca_temp_path:
        return ssl.create_default_context(cafile=ca_temp_path)


def test_mock_https_connection(httpserver, ca):
    """Test connections to a mock HTTPS job submission server."""
    httpserver.expect_request("/api/version").respond_with_json(
        {"ray_version": ray.__version__}
    )
    mock_url = httpserver.url_for("/")
    # Connection without SSL certificate should fail
    with pytest.raises(ConnectionError):
        JobSubmissionClient(mock_url)
    # Connecton with SSL verification skipped should succeed
    JobSubmissionClient(mock_url, verify=False)
    # Connection with SSL verification should succeed
    with ca.cert_pem.tempfile() as ca_temp_path:
        JobSubmissionClient(mock_url, verify=ca_temp_path)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
