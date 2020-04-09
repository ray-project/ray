package org.ray.streaming.runtime.master.resourcemanager;

import java.util.List;
import java.util.Map;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.ContainerID;

import static java.util.stream.Collectors.*;

/**
 * ViewBuilder describes current cluster's resource allocation detail information
 */
public class ViewBuilder {

  // Default constructor for serialization.
  public ViewBuilder() {
  }

  public static ResourceAssignmentView buildResourceAssignmentView(List<Container> containers) {
    Map<ContainerID, List<Integer>> assignmentView = containers.stream()
      .collect(toMap(Container::getId, Container::getExecutionVertexIds));

    return ResourceAssignmentView.of(assignmentView);
  }
}
