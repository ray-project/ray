package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ObjectId;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.functionmanager.FunctionDescriptor;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Task submitter for cluster mode. This is a wrapper class for core worker task interface.
 */
public class NativeTaskSubmitter implements TaskSubmitter {

  @Override
  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
                                   int numReturns, CallOptions options) {
    List<byte[]> returnIds = nativeSubmitTask(functionDescriptor, args, numReturns, options);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  @Override
  public BaseActorHandle createActor(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
                                     ActorCreationOptions options) {
    byte[] actorId = nativeCreateActor(functionDescriptor, args, options);
    return NativeActorHandle.create(actorId, functionDescriptor.getLanguage());
  }

  @Override
  public List<ObjectId> submitActorTask(
      BaseActorHandle actor, FunctionDescriptor functionDescriptor,
      List<FunctionArg> args, int numReturns, CallOptions options) {
    Preconditions.checkState(actor instanceof NativeActorHandle);
    List<byte[]> returnIds = nativeSubmitActorTask(actor.getId().getBytes(),
        functionDescriptor, args, numReturns, options);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  private static native List<byte[]> nativeSubmitTask(FunctionDescriptor functionDescriptor,
      List<FunctionArg> args, int numReturns, CallOptions callOptions);

  private static native byte[] nativeCreateActor(FunctionDescriptor functionDescriptor,
      List<FunctionArg> args, ActorCreationOptions actorCreationOptions);

  private static native List<byte[]> nativeSubmitActorTask(byte[] actorId,
      FunctionDescriptor functionDescriptor, List<FunctionArg> args, int numReturns,
      CallOptions callOptions);
}
