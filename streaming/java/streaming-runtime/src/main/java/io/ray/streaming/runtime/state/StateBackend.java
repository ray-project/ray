package io.ray.streaming.runtime.state;

import java.util.List;
import java.util.Map;

import io.ray.streaming.runtime.config.global.StateBackendConfig;

public interface StateBackend<K, V, C extends StateBackendConfig> {

  /**
   * init state backend
   *
   * @param config config
   */
  void init(final C config);

  /**
   * check if key exists in state
   *
   * @return true if exists
   */
  boolean exists(final K key) throws Exception;

  /**
   * get content by key
   *
   * @param key key
   * @return coStateBackendntent
   */
  V get(final K key) throws Exception;

  /**
   * get content by key in batch
   *
   * @param keys keys list
   * @return content list
   */
  List<V> batchGet(final List<K> keys) throws Exception;

  /**
   * put content by key
   *
   * @param key key
   * @param value content
   */
  void put(final K key, final V value) throws Exception;

  /**
   * put data in batch
   *
   * @throws Exception
   */
  void batchPut(final Map<K, V> batchData) throws Exception;

  /**
   * remove content by key
   *
   * @param key key
   */
  void remove(final K key) throws Exception;

  /**
   * flush
   */
  void flush() throws Exception;
}
