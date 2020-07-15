package io.ray.runtime.gcs;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.Checkpointable.Checkpoint;
import io.ray.api.id.ActorId;
import io.ray.api.id.BaseId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.runtime.generated.Gcs;
import io.ray.runtime.generated.Gcs.ActorCheckpointIdData;
import io.ray.runtime.generated.Gcs.GcsNodeInfo;
import io.ray.runtime.generated.Gcs.TablePrefix;
import io.ray.runtime.util.IdUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of GcsClient.
 */
public class GcsClient {
  private static Logger LOGGER = LoggerFactory.getLogger(GcsClient.class);
  private RedisClient primary;

  private List<RedisClient> shards;
  private GlobalStateAccessor globalStateAccessor;

  public GcsClient(String redisAddress, String redisPassword) {
    primary = new RedisClient(redisAddress, redisPassword);
    int numShards = 0;
    try {
      numShards = Integer.valueOf(primary.get("NumRedisShards", null));
      Preconditions.checkState(numShards > 0,
          String.format("Expected at least one Redis shards, found %d.", numShards));
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to get number of redis shards.", e);
    }

    List<byte[]> shardAddresses = primary.lrange("RedisShards".getBytes(), 0, -1);
    Preconditions.checkState(shardAddresses.size() == numShards);
    shards = shardAddresses.stream().map((byte[] address) -> {
      return new RedisClient(new String(address), redisPassword);
    }).collect(Collectors.toList());
    globalStateAccessor = GlobalStateAccessor.getInstance(redisAddress, redisPassword);
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
      final UniqueId nodeId = UniqueId
          .fromByteBuffer(data.getNodeId().asReadOnlyByteBuffer());

      if (data.getState() == GcsNodeInfo.GcsNodeState.ALIVE) {
        //Code path of node insertion.
        NodeInfo nodeInfo = new NodeInfo(
            nodeId, data.getNodeManagerAddress(),
            data.getNodeManagerHostname(),
            true, new HashMap<>());
        nodes.put(nodeId, nodeInfo);
      } else {
        // Code path of node deletion.
        NodeInfo nodeInfo = new NodeInfo(nodeId, nodes.get(nodeId).nodeAddress,
            nodes.get(nodeId).nodeHostname, false, new HashMap<>());
        nodes.put(nodeId, nodeInfo);
      }
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

  /**
   * If the actor exists in GCS.
   */
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

  /**
   * Query whether the raylet task exists in Gcs.
   */
  public boolean rayletTaskExistsInGcs(TaskId taskId) {
    byte[] key = ArrayUtils.addAll(TablePrefix.RAYLET_TASK.toString().getBytes(),
        taskId.getBytes());
    RedisClient client = getShardClient(taskId);
    return client.exists(key);
  }

  /**
   * Get the available checkpoints for the given actor ID.
   */
  public List<Checkpoint> getCheckpointsForActor(ActorId actorId) {
    List<Checkpoint> checkpoints = new ArrayList<>();
    byte[] result = globalStateAccessor.getActorCheckpointId(actorId);
    if (result != null) {
      ActorCheckpointIdData data = null;
      try {
        data = ActorCheckpointIdData.parseFrom(result);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Received invalid protobuf data from GCS.");
      }
      UniqueId[] checkpointIds = new UniqueId[data.getCheckpointIdsCount()];
      for (int i = 0; i < checkpointIds.length; i++) {
        checkpointIds[i] = UniqueId
            .fromByteBuffer(data.getCheckpointIds(i).asReadOnlyByteBuffer());
      }

      for (int i = 0; i < checkpointIds.length; i++) {
        checkpoints.add(new Checkpoint(checkpointIds[i], data.getTimestamps(i)));
      }
    }
    checkpoints.sort((x, y) -> Long.compare(y.timestamp, x.timestamp));
    return checkpoints;
  }

  public JobId nextJobId() {
    int jobCounter = (int) primary.incr("JobCounter".getBytes());
    return JobId.fromInt(jobCounter);
  }

  /**
   * Destroy global state accessor when ray native runtime will be shutdown.
   */
  public void destroy() {
    // Only ray shutdown should call gcs client destroy.
    LOGGER.debug("Destroying global state accessor.");
    GlobalStateAccessor.destroyInstance();
  }

  private RedisClient getShardClient(BaseId key) {
    return shards.get((int) Long.remainderUnsigned(IdUtil.murmurHashCode(key),
        shards.size()));
  }

}
