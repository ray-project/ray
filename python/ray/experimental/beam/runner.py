import itertools
import os
from google.protobuf import text_format
import grpc
import ray
import sys
import threading

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
from apache_beam.runners.portability.fn_api_runner.worker_handlers import WorkerHandler, GrpcWorkerHandler, _LOGGER
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner, _get_transform_overrides
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.utils import thread_pool_executor
from apache_beam.metrics import monitoring_infos
from apache_beam.transforms.environments import Environment


@Environment.register_urn("ray_worker_env", bytes)
class RayWorkerEnvironment(Environment):
  def __init__(
      self,
      command_string,  # type: str
      capabilities=(),  # type: Iterable[str]
      artifacts=(),  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
  ):
    super(RayWorkerEnvironment, self).__init__(capabilities, artifacts)
    self.command_string = command_string

  def __eq__(self, other):
    return self.__class__ == other.__class__ \
           and self.command_string == other.command_string

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    # type: () -> int
    return hash((self.__class__, self.command_string))

  def __repr__(self):
    # type: () -> str
    return 'RayWorkerEnvironment(command_string=%s)' % self.command_string

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, bytes]
    return "ray_worker_env", self.command_string.encode('utf-8')

  @staticmethod
  def from_runner_api_parameter(payload,  # type: bytes
                                capabilities,  # type: Iterable[str]
                                artifacts,  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
                                context  # type: PipelineContext
                               ):
    # type: (...) -> SubprocessSDKEnvironment
    return RayWorkerEnvironment(
        payload.decode('utf-8'), capabilities, artifacts)

  @classmethod
  def from_options(cls, options):
    # type: (PortableOptions) -> SubprocessSDKEnvironment
    return cls(
        options.environment_config,
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options))


@ray.remote
class BeamWorker(object):
  def __init__(self):
    print("Launching BEAM worker")
    from apache_beam.runners.worker.sdk_worker_main import main
    main([])

  def await_termination(self):
    return "exited"



from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import endpoints_pb2

class BeamFnLoggingServicer(beam_fn_api_pb2_grpc.BeamFnLoggingServicer):
  def Logging(self, log_bundles, context=None):
    for log_bundle in log_bundles:
      for log_entry in log_bundle.log_entries:
        _LOGGER.info('Worker: %s', str(log_entry).replace('\n', ' '))
    return iter([])


class RaySDKWorker(object):
  """Manages a SDK worker implemented as a Ray actor."""

  def __init__(
      self,
      control_address,
      worker_id=None):

    self._control_address = control_address
    self._worker_id = worker_id

  def run(self):
    logging_server = grpc.server(
        thread_pool_executor.shared_unbounded_instance())
    logging_port = logging_server.add_insecure_port('[::]:0')
    logging_server.start()
    logging_servicer = BeamFnLoggingServicer()
    beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
        logging_servicer, logging_server)
    logging_descriptor = text_format.MessageToString(
        endpoints_pb2.ApiServiceDescriptor(url='localhost:%s' % logging_port))

    control_descriptor = text_format.MessageToString(
        endpoints_pb2.ApiServiceDescriptor(url=self._control_address))

    env_dict = dict(
        os.environ,
        CONTROL_API_SERVICE_DESCRIPTOR=control_descriptor,
        LOGGING_API_SERVICE_DESCRIPTOR=logging_descriptor)
    # only add worker_id when it is set.
    if self._worker_id:
      env_dict['WORKER_ID'] = self._worker_id

    actor = BeamWorker.options(override_environment_variables=env_dict).remote()
    try:
      ray.get(actor.await_termination.remote())
    finally:
      logging_server.stop(0)


@WorkerHandler.register_environment("ray_worker_env", bytes)
class RayWorkerHandler(GrpcWorkerHandler):
  def __init__(self,
               worker_command_line,  # type: bytes
               state,  # type: StateServicer
               provision_info,  # type: ExtendedProvisionInfo
               grpc_server  # type: GrpcServer
              ):
    # type: (...) -> None
    super(RayWorkerHandler,
          self).__init__(state, provision_info, grpc_server)
    self._worker_command_line = worker_command_line

  def start_worker(self):

    self.worker = RaySDKWorker(self.control_address, self.worker_id)
    self.worker_thread = threading.Thread(
        name='run_worker', target=self.worker.run)
    self.worker_thread.start()
    print("Ray BEAM worker started")

  def stop_worker(self):
    print("Stop worker")
    self.worker_thread.join()


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
