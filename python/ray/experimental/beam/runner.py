from apache_beam.pipeline import PipelineVisitor
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from IPython import embed


class RayResult(PipelineResult):
    def __init__(self):
        pass

    def wait_until_finish(self, duration=None):
        print("wait_until_finish")


class Visitor(PipelineVisitor):
    def visit_transform(self, applied_ptransform):
        print("visit", applied_ptransform)


class RayRunner(PipelineRunner):
    def run_pipeline(self, pipeline, options):
        print("run_pipeline")
        pipeline.visit(Visitor())
        return RayResult()
