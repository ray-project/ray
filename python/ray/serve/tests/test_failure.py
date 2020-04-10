import time
import requests

from ray import serve
import ray


def _kill_http_proxy():
    [http_proxy] = ray.get(
        serve.api._get_master_actor().get_http_proxy.remote())
    ray.kill(http_proxy)


def request_with_retries(endpoint, verify_response, timeout=30):
    start = time.time()
    while True:
        try:
            verify_response(requests.get("http://127.0.0.1:8000" + endpoint))
            break
        except requests.RequestException:
            if time.time() - start > timeout:
                raise TimeoutError
            time.sleep(0.1)


def test_http_proxy_failure(serve_instance):
    serve.init()
    serve.create_endpoint("proxy_failure", "/proxy_failure", methods=["GET"])

    def function(flask_request):
        return "hello1"

    serve.create_backend(function, "failure:v1")
    serve.link("proxy_failure", "failure:v1")

    def verify_response(response):
        assert response.text == "hello1"

    request_with_retries("/proxy_failure", verify_response, timeout=0)

    _kill_http_proxy()

    request_with_retries("/proxy_failure", verify_response, timeout=30)

    _kill_http_proxy()

    def function(flask_request):
        return "hello2"

    serve.create_backend(function, "failure:v2")
    serve.link("proxy_failure", "failure:v2")

    def verify_response(response):
        assert response.text == "hello2"

    request_with_retries("/proxy_failure", verify_response, timeout=30)


def test_worker_failure(serve_instance):
    serve.init()
    serve.create_endpoint("worker_failure", "/worker_failure", methods=["GET"])

    def function(flask_request):
        return "hello1"

    serve.create_backend(function, "failure:v1")
    serve.link("worker_failure", "failure:v1")

    def verify_response(response):
        assert response.text == "hello1"

    request_with_retries("/worker_failure", verify_response, timeout=0)

    _kill_http_proxy()

    request_with_retries("/worker_failure", verify_response, timeout=30)

    _kill_http_proxy()

    def function(flask_request):
        return "hello2"

    serve.create_backend(function, "failure:v2")
    serve.link("worker_failure", "failure:v2")

    def verify_response(response):
        assert response.text == "hello2"

    request_with_retries("/worker_failure", verify_response, timeout=30)
