package io.ray.streaming.runtime.context;

import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.worker.JobWorker;

/**
 * This interface is used for storing context of {@link JobWorker} and {@link JobMaster}. The
 * checkpoint returned by user function is also saved using this interface.
 */
public interface ContextBackend {

  /**
   * check if key exists in state
   *
   * <p>Returns true if exists
   */
  boolean exists(final String key) throws Exception;

  /**
   * get content by key
   *
   * @param key key Returns the StateBackend
   */
  byte[] get(final String key) throws Exception;

  /**
   * put content by key
   *
   * @param key key
   * @param value content
   */
  void put(final String key, final byte[] value) throws Exception;

  /**
   * remove content by key
   *
   * @param key key
   */
  void remove(final String key) throws Exception;
}
