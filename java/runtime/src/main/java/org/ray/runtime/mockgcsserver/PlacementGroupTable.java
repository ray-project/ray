package org.ray.runtime.mockgcsserver;

import java.util.List;
import org.ray.api.id.GroupId;
import org.ray.api.options.PlacementStrategy;

public class PlacementGroupTable {

  public final GroupId id;

  public final String name;

  public final PlacementStrategy strategy;

  public final List<BundleTable> bundles;

  public PlacementGroupTable(GroupId id, String name, PlacementStrategy strategy,
      List<BundleTable> bundles) {
    this.id = id;
    this.name = name;
    this.strategy = strategy;
    this.bundles = bundles;
  }
}
