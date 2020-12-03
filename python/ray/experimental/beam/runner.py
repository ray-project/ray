from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner


class RayResult(PipelineResult):
    def __init__(self):
        pass

    def wait_until_finish(self, duration=None):
        print("wait_until_finish")


class RayRunner(PipelineRunner):
    def run_pipeline(self, pipeline, options):
        print("run_pipeline")
        return RayResult()
