package io.ray.streaming.runtime.context.impl;

import io.ray.streaming.runtime.config.global.ContextBackendConfig;
import io.ray.streaming.runtime.context.ContextBackend;
import java.io.File;
import org.apache.commons.io.FileUtils;

/**
 * This context backend uses local file system and doesn't supports failover in cluster. But it
 * supports failover in single node. This is a pure file system backend which doesn't support atomic
 * writing, please don't use this class, instead, use {@link AtomicFsBackend} which extends this
 * class.
 */
public class LocalFileContextBackend implements ContextBackend {

  private final String rootPath;

  public LocalFileContextBackend(ContextBackendConfig config) {
    rootPath = config.fileStateRootPath();
  }

  @Override
  public boolean exists(String key) {
    File file = new File(rootPath, key);
    return file.exists();
  }

  @Override
  public byte[] get(String key) throws Exception {
    File file = new File(rootPath, key);
    if (file.exists()) {
      return FileUtils.readFileToByteArray(file);
    }
    return null;
  }

  @Override
  public void put(String key, byte[] value) throws Exception {
    File file = new File(rootPath, key);
    FileUtils.writeByteArrayToFile(file, value);
  }

  @Override
  public void remove(String key) {
    File file = new File(rootPath, key);
    FileUtils.deleteQuietly(file);
  }

  protected void rename(String fromKey, String toKey) throws Exception {
    File srcFile = new File(rootPath, fromKey);
    File dstFile = new File(rootPath, toKey);
    FileUtils.moveFile(srcFile, dstFile);
  }
}
