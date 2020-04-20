package org.ray.runtime.mockgcsserver;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import org.ray.api.Bundle;
import org.ray.api.PlacementGroup;
import org.ray.api.Ray;
import org.ray.api.id.GroupId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.BundleOptions;
import org.ray.api.options.PlacementGroupOptions;
import org.ray.api.options.PlacementStrategy;
import org.ray.runtime.gcs.GcsClient;
import org.ray.runtime.group.NativeBundle;
import org.ray.runtime.group.NativePlacementGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockGcsServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockGcsServer.class);

  public static final Double LABEL_RESOURCE_AMOUNT = Math.pow(2, 15);

  private GcsClient gcsClient;

  private Map<GroupId, PlacementGroupTable> groups = new HashMap<>();

  private Map<GroupId, BundleTable> bundles = new HashMap<>();

  public MockGcsServer(GcsClient gcsClient) {
    this.gcsClient = gcsClient;
  }

  public void placeActor(ActorCreationOptions options) {
    if (options == null || options.bundle == null) {
      return;
    }

    BundleTable bundleTable = bundles.get(options.bundle.getId());
    Preconditions.checkNotNull(bundleTable, "Bundle %s does not exist.", options.bundle.getId());
    String label = allocateResources(bundleTable, options.resources);
    Preconditions
        .checkNotNull(label, "There is not enough resources in Bundle %s.", options.bundle.getId());
    options.resources.put(label, 1.0);
    LOGGER.info("Placed Actor in Bundle {} in Node labeled {}.", bundleTable.id, label);
  }

  private String allocateResources(BundleTable bundleTable, Map<String, Double> requirements) {
    LOGGER.info("Trying to allocate resources in Bundle {}.", bundleTable.id);
    for (UnitTable unitTable : bundleTable.units) {
      String label = allocateResources(unitTable, requirements);
      if (label != null) {
        return label;
      }
    }
    return null;
  }

  private String allocateResources(UnitTable unitTable, Map<String, Double> requirements) {
    boolean canAllocate = true;
    for (Map.Entry<String, Double> entry : requirements.entrySet()) {
      if (unitTable.availableResources.getOrDefault(entry.getKey(), 0.0) < entry.getValue()) {
        LOGGER.info("Resource \"{}\" = {} is not enough for requirement {}.", entry.getKey(),
            unitTable.availableResources.getOrDefault(entry.getKey(), 0.0), entry.getValue());
        canAllocate = false;
        break;
      }
    }

    if (canAllocate) {
      requirements.forEach(
          (k, v) -> unitTable.availableResources.computeIfPresent(k, (name, amount) -> amount - v));
      return unitTable.label;
    } else {
      return null;
    }
  }

  public PlacementGroup createPlacementGroup(PlacementGroupOptions options) {
    LOGGER.info("Creating a placement group with name \"{}\".", options.name);
    PlacementGroupTable groupTable = buildPlacementGroupTable(options);
    groups.put(groupTable.id, groupTable);
    LOGGER.info("Created placement group {} with name \"{}\".", groupTable.id, groupTable.name);
    return buildPlacementGroup(groupTable);
  }

  private PlacementGroupTable buildPlacementGroupTable(PlacementGroupOptions options) {
    // get node info
    List<NodeResource> nodes = getAllNodeResource();
    int nodeCount = nodes.size();
    int nodeIndex = new Random().nextInt(nodeCount);

    // construct bundles
    List<BundleTable> bundleTables = new ArrayList<>();
    for (BundleOptions bundleOptions : options.bundles) {
      List<UnitTable> unitTable = new ArrayList<>();
      // allocate resources one node by one node
      int remainingUnits = bundleOptions.unitCount;
      for (int i = 0; i < nodeCount; i++) {
        NodeResource node = nodes.get(nodeIndex % nodeCount);
        int allocated = preallocateResources(node, bundleOptions.resources, remainingUnits);
        IntStream.range(0, allocated).forEach(j -> unitTable
            .add(new UnitTable(new HashMap<>(bundleOptions.resources), node.getNodeLabel())));
        remainingUnits -= allocated;

        if (remainingUnits == 0) {
          break;
        }
        ++nodeIndex;
      }
      if (remainingUnits > 0) {
        throw new RuntimeException("There are not enough resources in this cluster.");
      }
      BundleTable bundleTable = new BundleTable(GroupId.fromRandom(), unitTable);
      bundles.put(bundleTable.id, bundleTable);
      bundleTables.add(bundleTable);
      LOGGER.info("Added Bundle {}.", bundleTable.id);

      if (options.strategy == PlacementStrategy.SPREAD) {
        ++nodeIndex;
      }
    }

    return new PlacementGroupTable(GroupId.fromRandom(), options.name, options.strategy,
        bundleTables);
  }

  private List<NodeResource> getAllNodeResource() {
    List<NodeResource> nodes = new ArrayList<>();
    gcsClient.getAllNodeInfo().forEach(nodeInfo -> {
      if (!nodeInfo.isAlive) {
        return;
      }
      LOGGER.info("Detected Node {}.", nodeInfo.nodeId);
      NodeResource node = new NodeResource(nodeInfo.nodeId, nodeInfo.resources);
      nodes.add(node);

      // set node label
      Ray.internal().setResource(node.getNodeLabel(), LABEL_RESOURCE_AMOUNT, nodeInfo.nodeId);
    });
    return nodes;
  }

  private int preallocateResources(NodeResource node, Map<String, Double> unitResources,
      int remainingUnits) {
    int minUnits = Integer.MAX_VALUE;
    for (Map.Entry<String, Double> entry : unitResources.entrySet()) {
      minUnits = Integer.min(minUnits, Double
          .valueOf(node.remainingResources.getOrDefault(entry.getKey(), 0.0) / entry.getValue())
          .intValue());
      if (minUnits == 0) {
        break;
      }
    }

    int allocatedUnits = Integer.min(minUnits, remainingUnits);
    if (allocatedUnits > 0) {
      unitResources.forEach((k, v) -> node.remainingResources
          .compute(k, (name, amount) -> amount - allocatedUnits * v));
    }

    LOGGER.info("Allocated {} / {} units in Node {}.", allocatedUnits, remainingUnits, node.nodeId);
    return allocatedUnits;
  }

  private PlacementGroup buildPlacementGroup(PlacementGroupTable groupTable) {
    List<Bundle> bundles = new ArrayList<>();
    groupTable.bundles.forEach(bundle -> bundles.add(new NativeBundle(bundle.id)));
    return new NativePlacementGroup(groupTable.name, groupTable.id, bundles);
  }
}
