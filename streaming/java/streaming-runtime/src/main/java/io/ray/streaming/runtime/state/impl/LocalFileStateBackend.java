package io.ray.streaming.runtime.state.impl;

import io.ray.streaming.runtime.config.global.StateBackendConfig;
import io.ray.streaming.runtime.state.StateBackend;
import java.io.File;
import org.apache.commons.io.FileUtils;

public class LocalFileStateBackend implements StateBackend {

  private final String rootPath;


  public LocalFileStateBackend(StateBackendConfig config) {
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
}
