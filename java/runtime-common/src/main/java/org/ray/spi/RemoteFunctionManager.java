package org.ray.spi;

import org.ray.api.UniqueID;
import org.ray.hook.runtime.LoadedFunctions;

/**
 * register and load functions from function table.
 */
public interface RemoteFunctionManager {

  /*
   * register <resourceId, resource> mapping, and upload resource.
   * this function is invoked by app proxy or other stand-alone tools it should detect for
   * duplication first though
   *
   * @param resourceZip a directory zip from @JarRewriter
   * @return SHA-1 hash of the content
   */
  UniqueID registerResource(byte[] resourceZip);

  /**
   * download resource content.
   *
   * @return resource content
   */
  byte[] getResource(UniqueID resourceId);

  /**
   * remove resource by its hash id
   * be careful of invoking this function to make sure it is no longer used.
   *
   * @param resourceId SHA-1 hash of the resource zip bytes
   */
  void unregisterResource(UniqueID resourceId);

  /*
   * register the <driver, resource> mapping to repo,
   * this function is invoked by whoever initiates the driver id
   */
  void registerApp(UniqueID driverId, UniqueID resourceId);

  /**
   * get the resourceId of one app.
   *
   * @return resourceId of the app driver
   */
  UniqueID getAppResourceId(UniqueID driverId);

  /*
   * unregister <dirver, resource> mapping
   * this function is called when the driver exits or detected dead
   */
  void unregisterApp(UniqueID driverId);

  /**
   * load resource and functions for this driver this function is used by the workers on demand when
   * a required function is not found in {@code LocalFunctionManager}.
   */
  LoadedFunctions loadFunctions(UniqueID driverId);

  /**
   * unload functions for this driver
   * this function is used by the workers on demand when a driver is dead.
   */
  void unloadFunctions(UniqueID driverId);
}
