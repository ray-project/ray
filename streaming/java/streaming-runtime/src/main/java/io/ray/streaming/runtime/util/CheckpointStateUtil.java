package io.ray.streaming.runtime.util;

import io.ray.streaming.runtime.context.ContextBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handle exception for checkpoint state */
public class CheckpointStateUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CheckpointStateUtil.class);

  /**
   * DO NOT ALLOW GET EXCEPTION WHEN LOADING CHECKPOINT
   *
   * @param checkpointState state backend
   * @param cpKey checkpoint key
   */
  public static byte[] get(ContextBackend checkpointState, String cpKey) {
    byte[] val;
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
   * @param checkpointState state backend
   * @param key checkpoint key
   * @param val checkpoint value
   */
  public static void put(ContextBackend checkpointState, String key, byte[] val) {
    try {
      checkpointState.put(key, val);
    } catch (Exception e) {
      LOG.error("Failed to put key {} to state backend.", key, e);
    }
  }

  public static class CheckpointStateRuntimeException extends RuntimeException {

    public CheckpointStateRuntimeException() {}

    public CheckpointStateRuntimeException(String message) {
      super(message);
    }

    public CheckpointStateRuntimeException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
