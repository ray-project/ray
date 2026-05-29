package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.ray.api.BaseActorHandle;
import io.ray.api.PlacementGroups;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.options.ActorCreationOptions;
import io.ray.api.options.CallOptions;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.functionmanager.FunctionDescriptor;
import io.ray.runtime.placementgroup.PlacementGroupImpl;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/** Task submitter for cluster mode. This is a wrapper class for core worker task interface. */
public class NativeTaskSubmitter implements TaskSubmitter {

  @Override
  public List<ObjectId> submitTask(
      FunctionDescriptor functionDescriptor,
      List<FunctionArg> args,
      int numReturns,
      CallOptions options) {
    List<byte[]> returnIds =
        nativeSubmitTask(
            functionDescriptor, functionDescriptor.hashCode(), args, numReturns, options);
    if (returnIds == null) {
      return ImmutableList.of();
    }
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  @Override
  public BaseActorHandle createActor(
      FunctionDescriptor functionDescriptor, List<FunctionArg> args, ActorCreationOptions options)
      throws IllegalArgumentException {
    if (options != null) {
      if (options.getGroup() != null) {
        PlacementGroupImpl group = (PlacementGroupImpl) options.getGroup();
        // bundleIndex == -1 indicates using any available bundle.
        Preconditions.checkArgument(
            options.getBundleIndex() == -1
                || options.getBundleIndex() >= 0
                    && options.getBundleIndex() < group.getBundles().size(),
            String.format(
                "Bundle index %s is invalid, the correct bundle index should be "
                    + "either in the range of 0 to the number of bundles "
                    + "or -1 which means put the task to any available bundles.",
                options.getBundleIndex()));
      }

      if (StringUtils.isNotBlank(options.getName())) {
        Optional<BaseActorHandle> actor = Ray.getActor(options.getName());
        Preconditions.checkArgument(
            !actor.isPresent(), String.format("Actor of name %s exists", options.getName()));
      }
    }
    byte[] actorId =
        nativeCreateActor(functionDescriptor, functionDescriptor.hashCode(), args, options);
    return NativeActorHandle.create(actorId, functionDescriptor.getLanguage());
  }

  @Override
  public BaseActorHandle getActor(ActorId actorId) {
    return NativeActorHandle.create(actorId.getBytes());
  }

  @Override
  public List<ObjectId> submitActorTask(
      BaseActorHandle actor,
      FunctionDescriptor functionDescriptor,
      List<FunctionArg> args,
      int numReturns,
      CallOptions options) {
    Preconditions.checkState(actor instanceof NativeActorHandle);
    // TODO: Ray Java does not support per-method MaxRetries. It only supports
    // setting Actor-level MaxTaskRetries for any method calls.
    // See: src/ray/core_worker/lib/java/io_ray_runtime_task_NativeTaskSubmitter.cc
    List<byte[]> returnIds =
        nativeSubmitActorTask(
            actor.getId().getBytes(),
            functionDescriptor,
            functionDescriptor.hashCode(),
            args,
            numReturns,
            options);
    if (returnIds == null) {
      return ImmutableList.of();
    }
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  @Override
  public PlacementGroup createPlacementGroup(PlacementGroupCreationOptions creationOptions) {
    if (StringUtils.isNotBlank(creationOptions.getName())) {
      PlacementGroup placementGroup = PlacementGroups.getPlacementGroup(creationOptions.getName());
      Preconditions.checkArgument(
          placementGroup == null,
          String.format("Placement group with name %s exists!", creationOptions.getName()));
    }
    byte[] bytes = nativeCreatePlacementGroup(creationOptions);
    return new PlacementGroupImpl.Builder()
        .setId(PlacementGroupId.fromBytes(bytes))
        .setName(creationOptions.getName())
        .setBundles(creationOptions.getBundles())
        .setStrategy(creationOptions.getStrategy())
        .build();
  }

  @Override
  public void removePlacementGroup(PlacementGroupId id) {
    nativeRemovePlacementGroup(id.getBytes());
  }

  @Override
  public boolean waitPlacementGroupReady(PlacementGroupId id, int timeoutSeconds) {
    return nativeWaitPlacementGroupReady(id.getBytes(), timeoutSeconds);
  }

  private static native List<byte[]> nativeSubmitTask(
      FunctionDescriptor functionDescriptor,
      int functionDescriptorHash,
      List<FunctionArg> args,
      int numReturns,
      CallOptions callOptions);

  private static native byte[] nativeCreateActor(
      FunctionDescriptor functionDescriptor,
      int functionDescriptorHash,
      List<FunctionArg> args,
      ActorCreationOptions actorCreationOptions);

  private static native List<byte[]> nativeSubmitActorTask(
      byte[] actorId,
      FunctionDescriptor functionDescriptor,
      int functionDescriptorHash,
      List<FunctionArg> args,
      int numReturns,
      CallOptions callOptions);

  private static native byte[] nativeCreatePlacementGroup(
      PlacementGroupCreationOptions creationOptions);

  private static native void nativeRemovePlacementGroup(byte[] placementGroupId);

  private static native boolean nativeWaitPlacementGroupReady(
      byte[] placementGroupId, int timeoutSeconds);
}
