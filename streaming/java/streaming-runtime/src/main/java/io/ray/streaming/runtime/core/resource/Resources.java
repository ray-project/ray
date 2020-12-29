package io.ray.streaming.runtime.core.resource;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.ray.api.id.UniqueId;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resource description of ResourceManager. */
public class Resources implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Resources.class);

  /** Available containers registered to ResourceManager. */
  private List<Container> registerContainers = new ArrayList<>();

  public Resources() {}

  /**
   * Get registered containers, the container list is read-only.
   *
   * <p>Returns container list.
   */
  public ImmutableList<Container> getRegisteredContainers() {
    return ImmutableList.copyOf(registerContainers);
  }

  public void registerContainer(Container container) {
    LOG.info("Add container {} to registry list.", container);
    this.registerContainers.add(container);
  }

  public void unRegisterContainer(List<UniqueId> deletedUniqueIds) {
    Iterator<Container> iter = registerContainers.iterator();
    while (iter.hasNext()) {
      Container deletedContainer = iter.next();
      if (deletedUniqueIds.contains(deletedContainer.getNodeId())) {
        LOG.info("Remove container {} from registry list.", deletedContainer);
        iter.remove();
      }
    }
  }

  public ImmutableMap<UniqueId, Container> getRegisteredContainerMap() {
    return ImmutableMap.copyOf(
        registerContainers.stream()
            .collect(java.util.stream.Collectors.toMap(Container::getNodeId, c -> c)));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("registerContainers", registerContainers)
        .toString();
  }
}
