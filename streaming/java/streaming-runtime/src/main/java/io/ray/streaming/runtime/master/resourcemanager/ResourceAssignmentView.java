package io.ray.streaming.runtime.master.resourcemanager;

import io.ray.streaming.runtime.core.resource.ContainerId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Cluster resource allocation view, used to statically view cluster resource information. */
public class ResourceAssignmentView extends HashMap<ContainerId, List<Integer>> {

  public static ResourceAssignmentView of(Map<ContainerId, List<Integer>> assignmentView) {
    ResourceAssignmentView view = new ResourceAssignmentView();
    view.putAll(assignmentView);
    return view;
  }
}
