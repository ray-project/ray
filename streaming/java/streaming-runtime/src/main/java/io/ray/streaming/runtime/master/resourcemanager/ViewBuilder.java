package io.ray.streaming.runtime.master.resourcemanager;

import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.core.resource.ContainerID;
import java.util.List;
import java.util.Map;

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
