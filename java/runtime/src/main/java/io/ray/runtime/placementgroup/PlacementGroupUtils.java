package io.ray.runtime.placementgroup;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.placementgroup.PlacementGroupState;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.runtime.generated.Common;
import io.ray.runtime.generated.Common.Bundle;
import io.ray.runtime.generated.Gcs.PlacementGroupTableData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Utils for placement group. */
public class PlacementGroupUtils {

  private static List<Map<String, Double>> covertToUserSpecifiedBundles(List<Bundle> bundles) {
    List<Map<String, Double>> result = new ArrayList<>();

    // NOTE(clay4444): We need to guarantee the order here.
    for (int i = 0; i < bundles.size(); i++) {
      Bundle bundle = bundles.get(i);
      result.add(bundle.getUnitResourcesMap());
    }
    return result;
  }

  private static PlacementStrategy covertToUserSpecifiedStrategy(
      Common.PlacementStrategy strategy) {
    switch (strategy) {
      case PACK:
        return PlacementStrategy.PACK;
      case STRICT_PACK:
        return PlacementStrategy.STRICT_PACK;
      case SPREAD:
        return PlacementStrategy.SPREAD;
      case STRICT_SPREAD:
        return PlacementStrategy.STRICT_SPREAD;
      default:
        return PlacementStrategy.UNRECOGNIZED;
    }
  }

  private static PlacementGroupState covertToUserSpecifiedState(
      PlacementGroupTableData.PlacementGroupState state) {
    switch (state) {
      case PENDING:
        return PlacementGroupState.PENDING;
      case CREATED:
        return PlacementGroupState.CREATED;
      case REMOVED:
        return PlacementGroupState.REMOVED;
      case RESCHEDULING:
        return PlacementGroupState.RESCHEDULING;
      default:
        return PlacementGroupState.UNRECOGNIZED;
    }
  }

  /**
   * Generate a PlacementGroupImpl from placementGroupTableData protobuf data.
   *
   * @param placementGroupTableData protobuf data. Returns placement group info {@link
   *     PlacementGroupImpl}
   */
  private static PlacementGroupImpl generatePlacementGroupFromPbData(
      PlacementGroupTableData placementGroupTableData) {

    PlacementGroupState state = covertToUserSpecifiedState(placementGroupTableData.getState());
    PlacementStrategy strategy =
        covertToUserSpecifiedStrategy(placementGroupTableData.getStrategy());

    List<Map<String, Double>> bundles =
        covertToUserSpecifiedBundles(placementGroupTableData.getBundlesList());

    PlacementGroupId placementGroupId =
        PlacementGroupId.fromByteBuffer(
            placementGroupTableData.getPlacementGroupId().asReadOnlyByteBuffer());

    return new PlacementGroupImpl.Builder()
        .setId(placementGroupId)
        .setName(placementGroupTableData.getName())
        .setState(state)
        .setStrategy(strategy)
        .setBundles(bundles)
        .build();
  }

  /**
   * Generate a PlacementGroupImpl from byte array.
   *
   * @param placementGroupByteArray bytes array from native method. Returns placement group info
   *     {@link PlacementGroupImpl}
   */
  public static PlacementGroupImpl generatePlacementGroupFromByteArray(
      byte[] placementGroupByteArray) {
    Preconditions.checkNotNull(
        placementGroupByteArray, "Can't generate a placement group from empty byte array.");

    PlacementGroupTableData placementGroupTableData;
    try {
      placementGroupTableData = PlacementGroupTableData.parseFrom(placementGroupByteArray);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Received invalid placement group table protobuf data from GCS.", e);
    }

    return generatePlacementGroupFromPbData(placementGroupTableData);
  }
}
