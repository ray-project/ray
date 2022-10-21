import locust
from locust import task, HttpUser
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, timeout, *args, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class ServeTest(HttpUser):
    wait_time = locust.wait_time.constant_throughput(10)

    def on_start(self):
        retries = Retry(
            total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504]
        )
        self.client.mount("http://", TimeoutHTTPAdapter(max_retries=retries, timeout=1))

    @task
    def index(self):
        self.client.get("/?val=3")
