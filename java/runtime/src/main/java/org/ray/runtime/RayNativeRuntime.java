package org.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.ray.api.Checkpointable.Checkpoint;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.generated.ActorCheckpointIdData;
import org.ray.runtime.generated.TablePrefix;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.RayletClientImpl;
import org.ray.runtime.runner.RunManager;
import org.ray.runtime.util.UniqueIdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * native runtime for local box and cluster run.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  /**
   * Redis client of the primary shard.
   */
  private RedisClient redisClient;
  /**
   * Redis clients of all shards.
   */
  private List<RedisClient> redisClients;
  private RunManager manager = null;

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private void resetLibraryPath() {
    String path = System.getProperty("java.library.path");
    if (Strings.isNullOrEmpty(path)) {
      path = "";
    } else {
      path += ":";
    }

    path += String.join(":", rayConfig.libraryPath);

    // This is a hack to reset library path at runtime,
    // see https://stackoverflow.com/questions/15409223/.
    System.setProperty("java.library.path", path);
    //set sys_paths to null so that java.library.path will be re-evalueted next time it is needed
    final Field sysPathsField;
    try {
      sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
      sysPathsField.setAccessible(true);
      sysPathsField.set(null, null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      e.printStackTrace();
      LOGGER.error("Failed to set library path.", e);
    }
  }

  @Override
  public void start() throws Exception {
    try {
      // Reset library path at runtime.
      resetLibraryPath();

      // Load native libraries.
      String[] libraries = new String[]{"raylet_library_java", "plasma_java"};
      for (String library : libraries) {
        String fileName = System.mapLibraryName(library);
        // Copy the file from resources to a temp dir, and load the native library.
        File file = File.createTempFile(fileName, "");
        file.deleteOnExit();
        InputStream in = RayNativeRuntime.class.getResourceAsStream("/" + fileName);
        Preconditions.checkNotNull(in, "{} doesn't exist.", fileName);
        Files.copy(in, Paths.get(file.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);
        System.load(file.getAbsolutePath());
      }
    } catch (Exception e) {
      LOGGER.error("Failed to load native libraries.", e);
      throw e;
    }

    if (rayConfig.getRedisAddress() == null) {
      manager = new RunManager(rayConfig);
      manager.startRayProcesses(true);
    }

    initRedisClients();

    // TODO(qwang): Get object_store_socket_name and raylet_socket_name from Redis.
    objectStoreProxy = new ObjectStoreProxy(this, rayConfig.objectStoreSocketName);

    rayletClient = new RayletClientImpl(
        rayConfig.rayletSocketName,
        workerContext.getCurrentWorkerId(),
        rayConfig.workerMode == WorkerMode.WORKER,
        workerContext.getCurrentDriverId()
    );

    // register
    registerWorker();

    LOGGER.info("RayNativeRuntime started with store {}, raylet {}",
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName);
  }

  private void initRedisClients() {
    redisClient = new RedisClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
    int numRedisShards = Integer.valueOf(redisClient.get("NumRedisShards", null));
    List<String> addresses = redisClient.lrange("RedisShards", 0, -1);
    Preconditions.checkState(numRedisShards == addresses.size());
    redisClients = addresses.stream().map(RedisClient::new)
        .collect(Collectors.toList());
    redisClients.add(redisClient);
  }

  @Override
  public void shutdown() {
    if (null != manager) {
      manager.cleanup();
    }
  }

  private void registerWorker() {
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(workerContext.getCurrentWorkerId().getBytes());
    if (rayConfig.workerMode == WorkerMode.DRIVER) {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("driver_id", workerId);
      workerInfo.put("start_time", String.valueOf(System.currentTimeMillis()));
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      workerInfo.put("name", System.getProperty("user.dir"));
      //TODO: worker.redis_client.hmset(b"Drivers:" + worker.workerId, driver_info)
      redisClient.hmset("Drivers:" + workerId, workerInfo);
    } else {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      //TODO: b"Workers:" + worker.workerId,
      redisClient.hmset("Workers:" + workerId, workerInfo);
    }
  }

  /**
   * Get the available checkpoints for the given actor ID, return a list sorted by checkpoint
   * timestamp in descending order.
   */
  List<Checkpoint> getCheckpointsForActor(UniqueId actorId) {
    List<Checkpoint> checkpoints = new ArrayList<>();
    // TODO(hchen): implement the equivalent of Python's `GlobalState`, to avoid looping over
    //  all redis shards..
    String prefix = TablePrefix.name(TablePrefix.ACTOR_CHECKPOINT_ID);
    byte[] key = ArrayUtils.addAll(prefix.getBytes(), actorId.getBytes());
    for (RedisClient client : redisClients) {
      byte[] result = client.get(key, null);
      if (result == null) {
        continue;
      }
      ActorCheckpointIdData data = ActorCheckpointIdData
          .getRootAsActorCheckpointIdData(ByteBuffer.wrap(result));

      UniqueId[] checkpointIds
          = UniqueIdUtil.getUniqueIdsFromByteBuffer(data.checkpointIdsAsByteBuffer());

      for (int i = 0; i < checkpointIds.length; i++) {
        checkpoints.add(new Checkpoint(checkpointIds[i], data.timestamps(i)));
      }
      break;
    }
    checkpoints.sort((x, y) -> Long.compare(y.timestamp, x.timestamp));
    return checkpoints;
  }


  /**
   * Query whether the actor exists in Gcs.
   */
  boolean actorExistsInGcs(UniqueId actorId) {
    byte[] key = ArrayUtils.addAll("ACTOR".getBytes(), actorId.getBytes());

    // TODO(qwang): refactor this with `GlobalState` after this issue
    // getting finished. https://github.com/ray-project/ray/issues/3933
    for (RedisClient client : redisClients) {
      if (client.exists(key)) {
        return true;
      }
    }

    return false;
  }
}
