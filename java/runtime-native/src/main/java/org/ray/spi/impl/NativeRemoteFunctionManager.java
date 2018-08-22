package org.ray.spi.impl;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import net.lingala.zip4j.core.ZipFile;
import org.ray.api.UniqueID;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.RemoteFunctionManager;
import org.ray.util.FileUtil;
import org.ray.util.Sha1Digestor;
import org.ray.util.SystemUtil;
import org.ray.util.logger.RayLog;

/**
 * native implementation of remote function manager.
 */
public class NativeRemoteFunctionManager implements RemoteFunctionManager {

  private final ConcurrentHashMap<UniqueID, ClassLoader> loadedApps = new ConcurrentHashMap<>();
  private MessageDigest md;
  private final String appDir = System.getProperty("user.dir") + "/apps";
  private final KeyValueStoreLink kvStore;


  public NativeRemoteFunctionManager(KeyValueStoreLink kvStore) throws NoSuchAlgorithmException {
    this.kvStore = kvStore;
    md = MessageDigest.getInstance("SHA-1");
    File appDir = new File(this.appDir);
    if (!appDir.exists()) {
      appDir.mkdirs();
    }

  }

  @Override
  public UniqueID registerResource(byte[] resourceZip) {
    byte[] digest = Sha1Digestor.digest(resourceZip);
    assert (digest.length == UniqueID.LENGTH);

    UniqueID resourceId = new UniqueID(digest);

    // TODO: resources must be saved in persistent store
    kvStore.set(resourceId.getBytes(), resourceZip, null);

    return resourceId;
  }

  @Override
  public byte[] getResource(UniqueID resourceId) {
    return kvStore.get(resourceId.getBytes(), null);
  }

  @Override
  public void unregisterResource(UniqueID resourceId) {
    kvStore.delete(resourceId.getBytes(), null);
  }

  @Override
  public void registerApp(UniqueID driverId, UniqueID resourceId) {
    kvStore.set("App2ResMap", resourceId.toString(), driverId.toString());
  }

  @Override
  public UniqueID getAppResourceId(UniqueID driverId) {
    return UniqueID.fromString(kvStore.get("App2ResMap", driverId.toString()));
  }

  @Override
  public void unregisterApp(UniqueID driverId) {
    kvStore.delete("App2ResMap", driverId.toString());
  }

  @Override
  public ClassLoader loadResource(UniqueID driverId) {
    ClassLoader classLoader = loadedApps.get(driverId);
    if (classLoader == null) {
      synchronized (this) {
        classLoader = loadedApps.get(driverId);
        if (classLoader == null) {
          classLoader = initLoadedApps(driverId);
        }
      }
    }
    return classLoader;
  }

  private ClassLoader initLoadedApps(UniqueID driverId) {
    try {
      RayLog.core.info("initLoadedApps" + driverId.toString());

      ClassLoader cl = loadedApps.get(driverId);
      if (cl == null) {
        UniqueID resId = UniqueID.fromString(kvStore.get("App2ResMap", driverId.toString()));
        byte[] res = getResource(resId);
        if (res == null) {
          throw new RuntimeException("get resource null, the resId " + resId.toString());
        }
        RayLog.core.info("get resource of " + resId.toString() + ", result len " + res.length);
        String resPath =
            appDir + "/" + driverId.toString() + "/" + String.valueOf(SystemUtil.pid());
        File dir = new File(resPath);
        if (!dir.exists()) {
          dir.mkdirs();
        }
        String zipPath = resPath + ".zip";
        RayLog.rapp.info("unzip app file: zipPath " + zipPath + " resPath " + resPath);
        FileUtil.bytesToFile(res, zipPath);
        ZipFile zipFile = new ZipFile(zipPath);
        zipFile.extractAll(resPath);
        cl = JarLoader.loadJars(resPath, false);
        loadedApps.put(driverId, cl);
      }
      return cl;
    } catch (Exception e) {
      RayLog.rapp.error("load function for " + driverId + " failed, ex = " + e.getMessage(), e);
      return null;
    }
  }

  @Override
  public synchronized void unloadFunctions(UniqueID driverId) {
    ClassLoader cl = loadedApps.get(driverId);
    try {
      JarLoader.unloadJars(cl);
    } catch (Exception e) {
      RayLog.rapp.error("unload function for " + driverId + " failed, ex = " + e.getMessage(), e);
    }
  }
}