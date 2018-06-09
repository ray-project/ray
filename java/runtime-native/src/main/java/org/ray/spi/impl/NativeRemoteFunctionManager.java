package org.ray.spi.impl;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import net.lingala.zip4j.core.ZipFile;
import org.ray.api.UniqueID;
import org.ray.core.RayRuntime;
import org.ray.hook.JarRewriter;
import org.ray.hook.runtime.JarLoader;
import org.ray.hook.runtime.LoadedFunctions;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.RemoteFunctionManager;
import org.ray.util.FileUtil;
import org.ray.util.SystemUtil;
import org.ray.util.logger.RayLog;

/**
 * native implementation of remote function manager.
 */
public class NativeRemoteFunctionManager implements RemoteFunctionManager {

  private ConcurrentHashMap<UniqueID, LoadedFunctions> loadedApps = new ConcurrentHashMap<>();
  private MessageDigest md;
  private String appDir = System.getProperty("user.dir") + "/apps";
  private KeyValueStoreLink kvStore;

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
    byte[] digest = md.digest(resourceZip);
    assert (digest.length == UniqueID.LENGTH);

    UniqueID resourceId = new UniqueID(digest);

    // TODO: resources must be saved in persistent store
    // instead of cache
    //if (!Ray.exist(resourceId)) {
    //Ray.put(resourceId, resourceZip);
    kvStore.set(resourceId.getBytes(), resourceZip, null);
    //}
    return resourceId;
  }

  @Override
  public byte[] getResource(UniqueID resourceId) {
    return kvStore.get(resourceId.getBytes(), null);
    //return (byte[])Ray.get(resourceId);
  }

  @Override
  public void unregisterResource(UniqueID resourceId) {
    kvStore.delete(resourceId.getBytes(), null);
  }

  @Override
  public void registerApp(UniqueID driverId, UniqueID resourceId) {
    //Ray.put(driverId, resourceId);
    kvStore.set("App2ResMap", resourceId.toString(), driverId.toString());
  }

  @Override
  public UniqueID getAppResourceId(UniqueID driverId) {
    return new UniqueID(kvStore.get("App2ResMap", driverId.toString()));
  }

  @Override
  public void unregisterApp(UniqueID driverId) {
    kvStore.delete("App2ResMap", driverId.toString());
  }

  @Override
  public LoadedFunctions loadFunctions(UniqueID driverId) {
    LoadedFunctions rf = loadedApps.get(driverId);
    if (rf == null) {
      rf = initLoadedApps(driverId);
    }
    return rf;
  }

  private synchronized LoadedFunctions initLoadedApps(UniqueID driverId) {
    try {
      RayLog.core.info("initLoadedApps" + driverId.toString());
      LoadedFunctions rf = loadedApps.get(driverId);
      if (rf == null) {
        UniqueID resId = new UniqueID(kvStore.get("App2ResMap", driverId.toString()));
        //UniqueID resId = Ray.get(driverId);

        byte[] res = getResource(resId);
        if (res == null) {
          throw new RuntimeException("get resource null, the resId " + resId.toString());
        }
        RayLog.core.info("ger resource of " + resId.toString() + ", result len " + res.length);
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
        rf = JarRewriter
            .load(resPath, RayRuntime.getInstance().getPaths().java_runtime_rewritten_jars_dir);
        loadedApps.put(driverId, rf);
      }
      return rf;
    } catch (Exception e) {
      RayLog.rapp.error("load function for " + driverId + " failed, ex = " + e.getMessage(), e);
      return null;
    }
  }

  @Override
  public synchronized void unloadFunctions(UniqueID driverId) {
    LoadedFunctions rf = loadedApps.get(driverId);
    try {
      JarLoader.unloadJars(rf.loader);
    } catch (Exception e) {
      RayLog.rapp.error("unload function for " + driverId + " failed, ex = " + e.getMessage(), e);
    }
  }
}
