package io.ray.streaming.runtime.context.impl;

import io.ray.streaming.runtime.config.global.ContextBackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Achieves an atomic `put` method. known issue: if you crashed while write a key at first time,
 * this code will not work.
 */
public class AtomicFsBackend extends LocalFileContextBackend {

  private static final Logger LOG = LoggerFactory.getLogger(AtomicFsBackend.class);
  private static final String TMP_FLAG = "_tmp";

  public AtomicFsBackend(final ContextBackendConfig config) {
    super(config);
  }

  @Override
  public byte[] get(String key) throws Exception {
    String tmpKey = key + TMP_FLAG;
    if (super.exists(tmpKey) && !super.exists(key)) {
      return super.get(tmpKey);
    }
    return super.get(key);
  }

  @Override
  public void put(String key, byte[] value) throws Exception {
    String tmpKey = key + TMP_FLAG;
    if (super.exists(tmpKey) && !super.exists(key)) {
      super.rename(tmpKey, key);
    }
    super.put(tmpKey, value);
    super.remove(key);
    super.rename(tmpKey, key);
  }

  @Override
  public void remove(String key) {
    String tmpKey = key + TMP_FLAG;
    if (super.exists(tmpKey)) {
      super.remove(tmpKey);
    }
    super.remove(key);
  }
}
