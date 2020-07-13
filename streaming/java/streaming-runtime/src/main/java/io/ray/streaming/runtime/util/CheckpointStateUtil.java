package io.ray.streaming.runtime.util;

import io.ray.streaming.runtime.config.global.StateBackendConfig;
import io.ray.streaming.runtime.state.StateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle exception for checkpoint state
 */
public class CheckpointStateUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CheckpointStateUtil.class);

  public static class CheckpointStateRuntimeException extends RuntimeException {

    public CheckpointStateRuntimeException() {
    }

    public CheckpointStateRuntimeException(String message) {
      super(message);
    }

    public CheckpointStateRuntimeException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * DO NOT ALLOW GET EXCEPTION WHEN LOADING CHECKPOINT
   *
   * @param checkpointState: statebackend
   * @param cpKey: checkpoint key
   */
  public static <K, V, C extends StateBackendConfig> V get(
    StateBackend<K, V, C> checkpointState, K cpKey) {
    V val;
    try {
      val = checkpointState.get(cpKey);
    } catch (Exception e) {
      throw new CheckpointStateRuntimeException(
          String.format("Failed to get %s from state backend.", cpKey), e);
    }
    return val;
  }

  /**
   * ALLOW PUT EXCEPTION WHEN SAVING CHECKPOINT
   *
   * @param checkpointState: statebackend
   * @param key: checkpoint key
   * @param val: checkpoint value
   * @param <K>: key generic type
   * @param <V>: value generic type
   */
  public static <K, V, C extends StateBackendConfig> void put(
      StateBackend<K, V, C> checkpointState, K key, V val) {
    try {
      checkpointState.put(key, val);
    } catch (Exception e) {
      // TODO: @dongxu.wdx we can return boolean to judge whether putting is succeed or not.
      LOG.error("Failed to put key {} to state backend.", key, e);
    }
  }
}
