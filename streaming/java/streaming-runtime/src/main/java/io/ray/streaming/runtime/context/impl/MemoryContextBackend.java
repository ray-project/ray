package io.ray.streaming.runtime.context.impl;

import io.ray.streaming.runtime.config.global.ContextBackendConfig;
import io.ray.streaming.runtime.context.ContextBackend;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This context backend uses memory and doesn't supports failover. Data will be lost after worker
 * died.
 */
public class MemoryContextBackend implements ContextBackend {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryContextBackend.class);

  private final Map<String, byte[]> kvStore = new HashMap<>();

  public MemoryContextBackend(ContextBackendConfig config) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Start init memory state backend, config is {}.", config);
      LOG.info("Finish init memory state backend.");
    }
  }

  @Override
  public boolean exists(String key) {
    return kvStore.containsKey(key);
  }

  @Override
  public byte[] get(final String key) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Get value of key {} start.", key);
    }

    byte[] readData = kvStore.get(key);

    if (LOG.isInfoEnabled()) {
      LOG.info("Get value of key {} success.", key);
    }

    return readData;
  }

  @Override
  public void put(final String key, final byte[] value) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Put value of key {} start.", key);
    }

    kvStore.put(key, value);

    if (LOG.isInfoEnabled()) {
      LOG.info("Put value of key {} success.", key);
    }
  }

  @Override
  public void remove(final String key) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Remove value of key {} start.", key);
    }

    kvStore.remove(key);

    if (LOG.isInfoEnabled()) {
      LOG.info("Remove value of key {} success.", key);
    }
  }
}
