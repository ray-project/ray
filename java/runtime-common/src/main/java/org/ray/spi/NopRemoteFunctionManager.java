package org.ray.spi;

import java.util.Set;
import org.ray.api.UniqueID;
import org.ray.hook.MethodId;
import org.ray.hook.runtime.LoadedFunctions;
import org.ray.util.logger.RayLog;

/**
 * mock version of remote function manager using local loaded jars + runtime hook.
 */
public class NopRemoteFunctionManager implements RemoteFunctionManager {

  private final LoadedFunctions loadedFunctions = new LoadedFunctions();

  public NopRemoteFunctionManager(UniqueID driverId) {
    //onLoad(driverId, Agent.hookedMethods);
    //Agent.consumers.add(m -> { this.onLoad(m); });
  }

  @Override
  public UniqueID registerResource(byte[] resourceZip) {
    return null;
    // nothing to do
  }

  @Override
  public byte[] getResource(UniqueID resourceId) {
    return null;
  }

  @Override
  public void unregisterResource(UniqueID resourceId) {
    // nothing to do
  }

  @Override
  public void registerApp(UniqueID driverId, UniqueID resourceId) {
    // nothing to do
  }

  @Override
  public UniqueID getAppResourceId(UniqueID driverId) {
    return null;
    // nothing to do
  }

  @Override
  public void unregisterApp(UniqueID driverId) {
    // nothing to do
  }

  @Override
  public LoadedFunctions loadFunctions(UniqueID driverId) {
    //assert (startupDriverId().equals(driverId));
    if (loadedFunctions == null) {
      RayLog.rapp.error("cannot find functions for " + driverId);
      return null;
    } else {
      return loadedFunctions;
    }
  }

  @Override
  public void unloadFunctions(UniqueID driverId) {
    // never
    //assert (startupDriverId().equals(driverId));
  }

  private void onLoad(UniqueID driverId, Set<MethodId> methods) {
    //assert (startupDriverId().equals(driverId));
    for (MethodId mid : methods) {
      onLoad(mid);
    }
  }

  private void onLoad(MethodId mid) {
    loadedFunctions.functions.add(mid);
  }
}
