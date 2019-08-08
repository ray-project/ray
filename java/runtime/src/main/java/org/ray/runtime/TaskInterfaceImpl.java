package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.api.id.ObjectId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.task.FunctionArg;

public class TaskInterfaceImpl implements TaskInterface {

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  public TaskInterfaceImpl(long nativeCoreWorkerPointer) {
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
  }

  @Override
  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      int numReturns, CallOptions options) {
    List<byte[]> returnIds = nativeSubmitTask(nativeCoreWorkerPointer, functionDescriptor, args,
        numReturns, options);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  @Override
  public RayActor createActor(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      ActorCreationOptions options) {
    long nativeActorHandle = nativeCreateActor(nativeCoreWorkerPointer, functionDescriptor, args,
        options);
    return new RayActorImpl(nativeActorHandle);
  }

  @Override
  public List<ObjectId> submitActorTask(RayActor actor, FunctionDescriptor functionDescriptor,
      List<FunctionArg> args, int numReturns, CallOptions options) {
    Preconditions.checkState(actor instanceof RayActorImpl);
    List<byte[]> returnIds = nativeSubmitActorTask(nativeCoreWorkerPointer,
        ((RayActorImpl) actor).getNativeActorHandle(), functionDescriptor, args, numReturns,
        options);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  private static native List<byte[]> nativeSubmitTask(long nativeCoreWorkerPointer,
      FunctionDescriptor functionDescriptor, List<FunctionArg> args, int numReturns,
      CallOptions callOptions);

  private static native long nativeCreateActor(long nativeCoreWorkerPointer,
      FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      ActorCreationOptions actorCreationOptions);

  private static native List<byte[]> nativeSubmitActorTask(long nativeCoreWorkerPointer,
      long nativeActorHandle, FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      int numReturns, CallOptions callOptions);
}
