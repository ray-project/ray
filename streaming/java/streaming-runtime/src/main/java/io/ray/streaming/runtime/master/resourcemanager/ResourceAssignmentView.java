package io.ray.streaming.runtime.master.resourcemanager;

import io.ray.streaming.runtime.core.resource.ContainerID;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cluster resource allocation view, used to statically view cluster resource information.
 */
public class ResourceAssignmentView extends HashMap<ContainerID, List<Integer>> {

  public static ResourceAssignmentView of(Map<ContainerID, List<Integer>> assignmentView) {
    ResourceAssignmentView view = new ResourceAssignmentView();
    view.putAll(assignmentView);
    return view;
  }
}
