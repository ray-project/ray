package org.ray.runtime.mockgcsserver;

import java.util.List;
import org.ray.api.id.GroupId;

public class BundleTable {

  public final GroupId id;

  public final List<UnitTable> units;

  public BundleTable(GroupId id, List<UnitTable> units) {
    this.id = id;
    this.units = units;
  }
}
