package org.ray.runtime;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
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
  private final long nativeCoreWorker;

  public TaskInterface(long nativeCoreWorker) {
    this.nativeCoreWorker = nativeCoreWorker;
  }

  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, FunctionArg[] args,
                                   int numReturns, CallOptions options) {
    NativeRayFunction nativeRayFunction = new NativeRayFunction(functionDescriptor);
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    NativeTaskOptions nativeTaskOptions = new NativeTaskOptions(numReturns, options);
    List<byte[]> returnIds = submitTask(nativeCoreWorker, nativeRayFunction, nativeArgs,
        nativeTaskOptions);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  public RayActorImpl<?> createActor(FunctionDescriptor functionDescriptor, FunctionArg[] args,
                                     ActorCreationOptions options) {
    NativeRayFunction nativeRayFunction = new NativeRayFunction(functionDescriptor);
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    NativeActorCreationOptions nativeActorCreationOptions = new NativeActorCreationOptions(options);
    return createActor(nativeCoreWorker, nativeRayFunction, nativeArgs,
        nativeActorCreationOptions);
  }

  public List<ObjectId> submitActorTask(RayActorImpl<?> actor, FunctionDescriptor functionDescriptor,
                                        FunctionArg[] args, int numReturns, CallOptions options) {
    NativeRayFunction nativeRayFunction = new NativeRayFunction(functionDescriptor);
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    NativeTaskOptions nativeTaskOptions = new NativeTaskOptions(numReturns, options);
    List<byte[]> returnIds = submitActorTask(nativeCoreWorker, actor.getNativeActorHandle(),
        nativeRayFunction, nativeArgs, nativeTaskOptions);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  private static native List<byte[]> submitTask(long nativeCoreWorker,
                                                NativeRayFunction rayFunction,
                                                List<NativeTaskArg> args,
                                                NativeTaskOptions taskOptions);

  private static native RayActorImpl<?> createActor(long nativeCoreWorker,
                                                    NativeRayFunction rayFunction,
                                                    List<NativeTaskArg> args,
                                                    NativeActorCreationOptions actorCreationOptions);

  private static native List<byte[]> submitActorTask(long nativeCoreWorker,
                                                     long nativeActorHandle,
                                                     NativeRayFunction rayFunction,
                                                     List<NativeTaskArg> args,
                                                     NativeTaskOptions taskOptions);
}
