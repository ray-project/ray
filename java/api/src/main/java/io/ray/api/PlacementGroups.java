package io.ray.api;

import io.ray.api.id.PlacementGroupId;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import java.util.List;

/** This class contains all public APIs of Placement Group. */
public class PlacementGroups {

  /**
   * Create a placement group. A placement group is used to place actors according to a specific
   * strategy and resource constraints. It will sends a request to GCS to preallocate the specified
   * resources, which is asynchronous. If the specified resource cannot be allocated, it will wait
   * for the resource to be updated and rescheduled.
   *
   * @param creationOptions Creation options of the placement group.
   * @return A handle to the created placement group.
   */
  public static PlacementGroup createPlacementGroup(PlacementGroupCreationOptions creationOptions) {
    return Ray.internal().createPlacementGroup(creationOptions);
  }

  /**
   * Get a placement group by placement group Id.
   *
   * @param id placement group id.
   * @return The placement group.
   */
  public static PlacementGroup getPlacementGroup(PlacementGroupId id) {
    return Ray.internal().getPlacementGroup(id);
  }

  /**
   * Get a placement group by placement group name from current job.
   *
   * @param name The placement group name.
   * @return The placement group.
   */
  public static PlacementGroup getPlacementGroup(String name) {
    return Ray.internal().getPlacementGroup(name, false);
  }

  /**
   * Get a placement group by placement group name from all jobs.
   *
   * @param name The placement group name.
   * @return The placement group.
   */
  public static PlacementGroup getGlobalPlacementGroup(String name) {
    return Ray.internal().getPlacementGroup(name, true);
  }

  /**
   * Get all placement groups in this cluster.
   *
   * @return All placement groups.
   */
  public static List<PlacementGroup> getAllPlacementGroups() {
    return Ray.internal().getAllPlacementGroups();
  }

  /**
   * Remove a placement group by id. Throw RayException if remove failed.
   *
   * @param id Id of the placement group.
   */
  public static void removePlacementGroup(PlacementGroupId id) {
    Ray.internal().removePlacementGroup(id);
  }
}
