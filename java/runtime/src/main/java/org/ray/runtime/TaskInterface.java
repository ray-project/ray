package org.ray.runtime;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.id.ObjectId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.nativeTypes.NativeActorCreationOptions;
import org.ray.runtime.nativeTypes.NativeRayFunction;
import org.ray.runtime.nativeTypes.NativeTaskArg;
import org.ray.runtime.nativeTypes.NativeTaskOptions;
import org.ray.runtime.task.FunctionArg;

public class TaskInterface {

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  public TaskInterface(long nativeCoreWorkerPointer) {
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
  }

  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, FunctionArg[] args,
      int numReturns, CallOptions options) {
    NativeRayFunction nativeRayFunction = new NativeRayFunction(functionDescriptor);
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    NativeTaskOptions nativeTaskOptions = new NativeTaskOptions(numReturns, options);
    List<byte[]> returnIds = nativeSubmitTask(nativeCoreWorkerPointer, nativeRayFunction, nativeArgs,
        nativeTaskOptions);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  public RayActorImpl createActor(FunctionDescriptor functionDescriptor, FunctionArg[] args,
      ActorCreationOptions options) {
    NativeRayFunction nativeRayFunction = new NativeRayFunction(functionDescriptor);
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    NativeActorCreationOptions nativeActorCreationOptions = new NativeActorCreationOptions(options);
    long nativeActorHandle = nativeCreateActor(nativeCoreWorkerPointer,
        nativeRayFunction, nativeArgs, nativeActorCreationOptions);
    return new RayActorImpl(nativeActorHandle);
  }

  public List<ObjectId> submitActorTask(RayActorImpl actor, FunctionDescriptor functionDescriptor,
      FunctionArg[] args, int numReturns, CallOptions options) {
    NativeRayFunction nativeRayFunction = new NativeRayFunction(functionDescriptor);
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    NativeTaskOptions nativeTaskOptions = new NativeTaskOptions(numReturns, options);
    List<byte[]> returnIds = nativeSubmitActorTask(nativeCoreWorkerPointer,
        actor.getNativeActorHandle(),
        nativeRayFunction, nativeArgs, nativeTaskOptions);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  private static native List<byte[]> nativeSubmitTask(long nativeCoreWorkerPointer,
      NativeRayFunction rayFunction, List<NativeTaskArg> args, NativeTaskOptions taskOptions);

  private static native long nativeCreateActor(long nativeCoreWorkerPointer,
      NativeRayFunction rayFunction, List<NativeTaskArg> args,
      NativeActorCreationOptions actorCreationOptions);

  private static native List<byte[]> nativeSubmitActorTask(long nativeCoreWorkerPointer,
      long nativeActorHandle, NativeRayFunction rayFunction, List<NativeTaskArg> args,
      NativeTaskOptions taskOptions);
}
