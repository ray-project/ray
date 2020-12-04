import itertools
import sys

from apache_beam.pipeline import PipelineVisitor
from apache_beam.options.pipeline_options import DirectOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.runners.direct.bundle_factory import BundleFactory
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.direct.clock import RealClock
from apache_beam.portability.api import beam_fn_api_pb2, beam_runner_api_pb2
from apache_beam.portability import python_urns
from apache_beam.runners.portability.fn_api_runner import FnApiRunner
from apache_beam.runners.portability.fn_api_runner.fn_runner import BundleManager
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner, _get_transform_overrides
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.utils import thread_pool_executor
from apache_beam.metrics import monitoring_infos


class MyParallelBundleManager(BundleManager):

  def __init__(
      self,
      bundle_context_manager,  # type: execution.BundleContextManager
      progress_frequency=None,  # type: Optional[float]
      cache_token_generator=None,
      **kwargs):
    # type: (...) -> None
    super(MyParallelBundleManager, self).__init__(
        bundle_context_manager,
        progress_frequency,
        cache_token_generator=cache_token_generator)
    self._num_workers = bundle_context_manager.num_workers

  def process_bundle(self,
                     inputs,  # type: Mapping[str, execution.PartitionableBuffer]
                     expected_outputs,  # type: DataOutput
                     fired_timers,  # type: Mapping[Tuple[str, str], execution.PartitionableBuffer]
                     expected_output_timers,  # type: OutputTimers
                     dry_run=False,  # type: bool
                    ):
    # type: (...) -> BundleProcessResult
    part_inputs = [{} for _ in range(self._num_workers)
                   ]  # type: List[Dict[str, List[bytes]]]
    # Timers are only executed on the first worker
    # TODO(BEAM-9741): Split timers to multiple workers
    timer_inputs = [
        fired_timers if i == 0 else {} for i in range(self._num_workers)
    ]
    for name, input in inputs.items():
      for ix, part in enumerate(input.partition(self._num_workers)):
        part_inputs[ix][name] = part

    merged_result = None  # type: Optional[beam_fn_api_pb2.InstructionResponse]
    split_result_list = [
    ]  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]

    def execute(part_map_input_timers):
      # type: (...) -> BundleProcessResult
      part_map, input_timers = part_map_input_timers
      bundle_manager = BundleManager(
          self.bundle_context_manager,
          self._progress_frequency,
          cache_token_generator=self._cache_token_generator)
      return bundle_manager.process_bundle(
          part_map,
          expected_outputs,
          input_timers,
          expected_output_timers,
          dry_run)

    print("=== PART INPUTS ({}) ===".format(len(part_inputs)))
    print(part_inputs)
    print("===================")
    with thread_pool_executor.shared_unbounded_instance() as executor:
      for result, split_result in executor.map(execute, zip(part_inputs, timer_inputs)):
        #print("===== result below =====:")
        #print(result)
        #print("===== result above =====:")
        split_result_list += split_result
        if merged_result is None:
          merged_result = result
        else:
          merged_result = beam_fn_api_pb2.InstructionResponse(
              process_bundle=beam_fn_api_pb2.ProcessBundleResponse(
                  monitoring_infos=monitoring_infos.consolidate(
                      itertools.chain(
                          result.process_bundle.monitoring_infos,
                          merged_result.process_bundle.monitoring_infos))),
              error=result.error or merged_result.error)
    assert merged_result is not None
    return merged_result, split_result_list

#import apache_beam.runners.portability.fn_api_runner.fn_runner #ParallelBundleManager
#apache_beam.runners.portability.fn_api_runner.fn_runner.ParallelBundleManager = MyParallelBundleManager
#apache_beam.runners.portability.fn_api_runner.fn_runner.BundleManager = None


class Visitor(PipelineVisitor):
    def visit_transform(self, applied_ptransform):
        print("visit", applied_ptransform)
        print(applied_ptransform.__dict__)


class RayRunner(PipelineRunner):
    def run_pipeline(self, pipeline, options):
        #print("run_pipeline")
        #print(pipeline.to_runner_api())
        #pipeline.visit(Visitor())
        return FnApiRunner().run_pipeline(pipeline, options)
