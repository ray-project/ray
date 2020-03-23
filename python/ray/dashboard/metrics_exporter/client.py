import json
import logging
import re
import requests

from ray.dashboard.metrics_exporter.api import post_auth
from ray.dashboard.metrics_exporter.exporter import Exporter

logger = logging.getLogger(__name__)


# Copied from Django URL validation.
def validate_url(url):
    regex = re.compile(
            r'^(?:http)s?://' # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
            r'localhost|' #localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
            r'(?::\d+)?' # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    if re.match(regex, url) is None:
        raise ValueError("URL {} is invalid. "
                         "Follow http(s)://(domain|localhost|ip):[port][/]"
                         .format(url))
    return url


class MetricsExportClient:
    """Manages the communication to external services to export metrics.

    This client must be a singleton because start_export_metrics should not be
    called more than once as it can create multiple export metrics threads.

    Args:
        address: Address to export metrics
        dashboard_controller(BaseDashboardController): Dashboard controller to
            run dashboard business logic.
        dashboard_id(str): Unique dashboard ID.
    """

    def __init__(self, address, dashboard_controller, dashboard_id):
        self.dashboard_id = dashboard_id
        self.dashboard_controller = dashboard_controller
        self.exporter = None

        # URLs
        self.url = validate_url(address)
        self.auth_url = "{}/auth".format(self.url)
        self.ingestor_url = "{}/ingest".format(self.url)

        # Data obtained from requests.
        self._dashboard_url = None        
        self.auth_info = None

        # Client states
        self.is_authenticated = False
        self.is_exporting_started = False

    def _authenticate(self):
        """
        Return:
            Whether or not the authentication succeed.
        """
        auth_info = post_auth(self.auth_url, self.dashboard_id)
        if auth_info is None:
            return False
        
        dashboard_url = auth_info.get("dashboard_url", None)
        # TODO(sang): Switch to schema validation.
        if dashboard_url is None:
            logger.error("Dashboard URL wasn't included in auth info {}"
                        .format(self.auth_info))
            return False

        self.auth_info = auth_info
        self._dashboard_url = dashboard_url
        self.is_authenticated = True
        return True

    @property
    def enabled(self):
        return self.is_authenticated

    @property
    def dashboard_url(self):
        return self._dashboard_url

    def start_exporting_metrics(self):
        """Create a thread to export metrics. 

        Once this function succeeds, it should not be called again.

        Return:
            Whether or not it suceedes to run exporter.
        """
        assert not self.is_exporting_started
        if not self.is_authenticated:
            succeed = self._authenticate()
            if not succeed: 
                return False

        # Exporter is a Python thread that keeps exporting metrics with
        # access token obtained by an authentication process.
        self.exporter = Exporter(self.dashboard_id, self.ingestor_url,
                                 self.auth_info.get("access_token", None),
                                 self.dashboard_controller)
        self.exporter.start()
        self.is_exporting_started = True
        return True
