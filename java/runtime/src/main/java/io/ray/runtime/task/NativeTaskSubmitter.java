package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.functionmanager.FunctionDescriptor;
import io.ray.runtime.placementgroup.PlacementGroupId;
import io.ray.runtime.placementgroup.PlacementGroupImpl;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Task submitter for cluster mode. This is a wrapper class for core worker task interface.
 */
public class NativeTaskSubmitter implements TaskSubmitter {

  @Override
  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
                                   int numReturns, CallOptions options) {
    List<byte[]> returnIds = nativeSubmitTask(functionDescriptor, functionDescriptor.hashCode(),
        args, numReturns, options);
    if (returnIds == null) {
      return ImmutableList.of();
    }
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  @Override
  public BaseActorHandle createActor(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
                                     ActorCreationOptions options) throws IllegalArgumentException {
    if (options != null) {
      if (options.group != null) {
        PlacementGroupImpl group = (PlacementGroupImpl)options.group;
        Preconditions.checkArgument(options.bundleIndex >= 0
                && options.bundleIndex < group.getBundleCount(),
            String.format("Bundle index %s is invalid", options.bundleIndex));
      }

      if (StringUtils.isNotBlank(options.name)) {
        Optional<BaseActorHandle> actor =
            options.global ? Ray.getGlobalActor(options.name) : Ray.getActor(options.name);
        Preconditions.checkArgument(!actor.isPresent(),
            String.format("Actor of name %s exists", options.name));
      }
    }
    byte[] actorId = nativeCreateActor(functionDescriptor, functionDescriptor.hashCode(), args,
        options);
    return NativeActorHandle.create(actorId, functionDescriptor.getLanguage());
  }

  @Override
  public BaseActorHandle getActor(ActorId actorId) {
    return NativeActorHandle.create(actorId.getBytes());
  }

  @Override
  public List<ObjectId> submitActorTask(
      BaseActorHandle actor, FunctionDescriptor functionDescriptor,
      List<FunctionArg> args, int numReturns, CallOptions options) {
    Preconditions.checkState(actor instanceof NativeActorHandle);
    List<byte[]> returnIds = nativeSubmitActorTask(actor.getId().getBytes(),
        functionDescriptor, functionDescriptor.hashCode(), args, numReturns, options);
    if (returnIds == null) {
      return ImmutableList.of();
    }
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  @Override
  public PlacementGroup createPlacementGroup(List<Map<String, Double>> bundles,
      PlacementStrategy strategy) {
    byte[] bytes = nativeCreatePlacementGroup(bundles, strategy.value());
    return new PlacementGroupImpl(PlacementGroupId.fromBytes(bytes), bundles.size());
  }

  private static native List<byte[]> nativeSubmitTask(FunctionDescriptor functionDescriptor,
      int functionDescriptorHash, List<FunctionArg> args, int numReturns, CallOptions callOptions);

  private static native byte[] nativeCreateActor(FunctionDescriptor functionDescriptor,
      int functionDescriptorHash, List<FunctionArg> args,
      ActorCreationOptions actorCreationOptions);

  private static native List<byte[]> nativeSubmitActorTask(byte[] actorId,
      FunctionDescriptor functionDescriptor, int functionDescriptorHash, List<FunctionArg> args,
      int numReturns, CallOptions callOptions);

  private static native byte[] nativeCreatePlacementGroup(List<Map<String, Double>> bundles,
      int strategy);
}
