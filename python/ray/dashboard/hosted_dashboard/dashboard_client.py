from ray.dashboard.hosted_dashboard.exporter import Exporter


class DashboardClient:
    """Managing communication to hosted dashboard.

    Attributes:
        ingestor_url(str): Address that metrics will be exported.
        exporter(Exporter): Exporter thread that keeps exporting
            metrics to the external services.
    """

    def __init__(self, dashboard_controller):
        # TODO(sang): Remove hard coded ingestor url.
        self.ingestor_url = "127.0.0.1:50051"
        self.exporter = Exporter(self.ingestor_url, dashboard_controller)

    def start_exporting_metrics(self):
        """Run an exporter thread to export metrics"""
        # TODO(sang): Add a health check.
        self.exporter.start()
