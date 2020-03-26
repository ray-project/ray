package org.ray.runtime.task;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.BaseActor;
import org.ray.api.id.ObjectId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.runtime.actor.NativeRayActor;
import org.ray.runtime.functionmanager.FunctionDescriptor;

/**
 * Task submitter for cluster mode. This is a wrapper class for core worker task interface.
 */
public class NativeTaskSubmitter implements TaskSubmitter {

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

  public NativeTaskSubmitter(long nativeCoreWorkerPointer) {
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
  public BaseActor createActor(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
                              ActorCreationOptions options) {
    byte[] actorId = nativeCreateActor(nativeCoreWorkerPointer, functionDescriptor, args,
        options);
    return NativeRayActor.create(nativeCoreWorkerPointer, actorId,
        functionDescriptor.getLanguage());
  }

  @Override
  public List<ObjectId> submitActorTask(
      BaseActor actor, FunctionDescriptor functionDescriptor,
      List<FunctionArg> args, int numReturns, CallOptions options) {
    Preconditions.checkState(actor instanceof NativeRayActor);
    List<byte[]> returnIds = nativeSubmitActorTask(nativeCoreWorkerPointer,
        actor.getId().getBytes(), functionDescriptor, args, numReturns,
        options);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  private static native List<byte[]> nativeSubmitTask(
      long nativeCoreWorkerPointer,
      FunctionDescriptor functionDescriptor, List<FunctionArg> args, int numReturns,
      CallOptions callOptions);

  private static native byte[] nativeCreateActor(
      long nativeCoreWorkerPointer,
      FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      ActorCreationOptions actorCreationOptions);

  private static native List<byte[]> nativeSubmitActorTask(
      long nativeCoreWorkerPointer,
      byte[] actorId, FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      int numReturns, CallOptions callOptions);
}
