package org.ray.streaming.runtime.master.resourcemanager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.streaming.runtime.core.resource.ContainerID;

public class ResourceAssignmentView extends HashMap<ContainerID, List<Integer>> {

  public static ResourceAssignmentView of(Map<ContainerID, List<Integer>> assignmentView) {
    ResourceAssignmentView view = new ResourceAssignmentView();
    view.putAll(assignmentView);
    return view;
  }
}
