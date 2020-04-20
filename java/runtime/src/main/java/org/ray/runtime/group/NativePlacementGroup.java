package org.ray.runtime.group;

import java.util.List;
import org.ray.api.Bundle;
import org.ray.api.PlacementGroup;
import org.ray.api.id.GroupId;

public class NativePlacementGroup implements PlacementGroup {

  public final String name;

  public final GroupId id;

  public final List<Bundle> bundles;

  public NativePlacementGroup(String name, GroupId id, List<Bundle> bundles) {
    this.name = name;
    this.id = id;
    this.bundles = bundles;
  }

  @Override
  public GroupId getId() {
    return id;
  }

  @Override
  public List<Bundle> getBundles() {
    return bundles;
  }
}
