package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.id.UniqueId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.runtime.generated.Gcs;
import io.ray.runtime.generated.Gcs.GcsNodeInfo;
import io.ray.runtime.generated.Gcs.TablePrefix;
import io.ray.runtime.placementgroup.PlacementGroupUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of GcsClient. */
public class GcsClient {
  private static Logger LOGGER = LoggerFactory.getLogger(GcsClient.class);
  private RedisClient primary;

  private GlobalStateAccessor globalStateAccessor;

  public GcsClient(String redisAddress, String redisPassword) {
    primary = new RedisClient(redisAddress, redisPassword);
    globalStateAccessor = GlobalStateAccessor.getInstance(redisAddress, redisPassword);
  }

  /**
   * Get placement group by {@link PlacementGroupId}.
   *
   * @param placementGroupId Id of placement group.
   * @return The placement group.
   */
  public PlacementGroup getPlacementGroupInfo(PlacementGroupId placementGroupId) {
    byte[] result = globalStateAccessor.getPlacementGroupInfo(placementGroupId);
    return PlacementGroupUtils.generatePlacementGroupFromByteArray(result);
  }

  /**
   * Get all placement groups in this cluster.
   *
   * @return All placement groups.
   */
  public List<PlacementGroup> getAllPlacementGroupInfo() {
    List<byte[]> results = globalStateAccessor.getAllPlacementGroupInfo();

    List<PlacementGroup> placementGroups = new ArrayList<>();
    for (byte[] result : results) {
      placementGroups.add(PlacementGroupUtils.generatePlacementGroupFromByteArray(result));
    }
    return placementGroups;
  }

  public List<NodeInfo> getAllNodeInfo() {
    List<byte[]> results = globalStateAccessor.getAllNodeInfo();

    // This map is used for deduplication of node entries.
    Map<UniqueId, NodeInfo> nodes = new HashMap<>();
    for (byte[] result : results) {
      Preconditions.checkNotNull(result);
      GcsNodeInfo data = null;
      try {
        data = GcsNodeInfo.parseFrom(result);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Received invalid protobuf data from GCS.");
      }
      final UniqueId nodeId = UniqueId.fromByteBuffer(data.getNodeId().asReadOnlyByteBuffer());

      // NOTE(lingxuan.zlx): we assume no duplicated node id in fetched node list
      // and it's only one final state for each node in recorded table.
      NodeInfo nodeInfo =
          new NodeInfo(
              nodeId,
              data.getNodeManagerAddress(),
              data.getNodeManagerHostname(),
              data.getNodeManagerPort(),
              data.getObjectStoreSocketName(),
              data.getRayletSocketName(),
              data.getState() == GcsNodeInfo.GcsNodeState.ALIVE,
              new HashMap<>());
      nodes.put(nodeId, nodeInfo);
    }

    // Fill resources.
    for (Map.Entry<UniqueId, NodeInfo> node : nodes.entrySet()) {
      if (node.getValue().isAlive) {
        node.getValue().resources.putAll(getResourcesForClient(node.getKey()));
      }
    }

    return new ArrayList<>(nodes.values());
  }

  public Map<String, String> getInternalConfig() {
    Gcs.StoredConfig storedConfig;
    byte[] conf = globalStateAccessor.getInternalConfig();
    try {
      storedConfig = Gcs.StoredConfig.parseFrom(conf);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Received invalid internal config protobuf from GCS.");
    }
    return storedConfig.getConfigMap();
  }

  private Map<String, Double> getResourcesForClient(UniqueId clientId) {
    byte[] resourceMapBytes = globalStateAccessor.getNodeResourceInfo(clientId);
    Gcs.ResourceMap resourceMap;
    try {
      resourceMap = Gcs.ResourceMap.parseFrom(resourceMapBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Received invalid protobuf data from GCS.");
    }
    HashMap<String, Double> resources = new HashMap<>();
    for (Map.Entry<String, Gcs.ResourceTableData> entry : resourceMap.getItemsMap().entrySet()) {
      resources.put(entry.getKey(), entry.getValue().getResourceCapacity());
    }
    return resources;
  }

  /** If the actor exists in GCS. */
  public boolean actorExists(ActorId actorId) {
    byte[] result = globalStateAccessor.getActorInfo(actorId);
    return result != null;
  }

  public boolean wasCurrentActorRestarted(ActorId actorId) {
    byte[] key = ArrayUtils.addAll(TablePrefix.ACTOR.toString().getBytes(), actorId.getBytes());

    // TODO(ZhuSenlin): Get the actor table data from CoreWorker later.
    byte[] value = globalStateAccessor.getActorInfo(actorId);
    if (value == null) {
      return false;
    }
    Gcs.ActorTableData actorTableData = null;
    try {
      actorTableData = Gcs.ActorTableData.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Received invalid protobuf data from GCS.");
    }
    return actorTableData.getNumRestarts() != 0;
  }

  public JobId nextJobId() {
    int jobCounter = (int) primary.incr("JobCounter".getBytes());
    return JobId.fromInt(jobCounter);
  }

  /** Destroy global state accessor when ray native runtime will be shutdown. */
  public void destroy() {
    // Only ray shutdown should call gcs client destroy.
    LOGGER.debug("Destroying global state accessor.");
    GlobalStateAccessor.destroyInstance();
  }
}
