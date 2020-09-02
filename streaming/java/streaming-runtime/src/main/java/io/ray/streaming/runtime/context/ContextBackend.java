package io.ray.streaming.runtime.context;

public interface ContextBackend {

  /**
   * check if key exists in state
   *
   * @return true if exists
   */
  boolean exists(final String key) throws Exception;

  /**
   * get content by key
   *
   * @param key key
   * @return the StateBackend
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
