package org.ray.spi;

import org.ray.api.id.UniqueId;

/**
 * mock version of remote function manager using local loaded jars.
 */
public class NopRemoteFunctionManager implements RemoteFunctionManager {

  public NopRemoteFunctionManager(UniqueId driverId) {
    //onLoad(driverId, Agent.hookedMethods);
    //Agent.consumers.add(m -> { this.onLoad(m); });
  }

  @Override
  public UniqueId registerResource(byte[] resourceZip) {
    return null;
    // nothing to do
  }

  @Override
  public byte[] getResource(UniqueId resourceId) {
    return null;
  }

  @Override
  public void unregisterResource(UniqueId resourceId) {
    // nothing to do
  }

  @Override
  public void registerApp(UniqueId driverId, UniqueId resourceId) {
    // nothing to do
  }

  @Override
  public UniqueId getAppResourceId(UniqueId driverId) {
    return null;
    // nothing to do
  }

  @Override
  public void unregisterApp(UniqueId driverId) {
    // nothing to do
  }

  @Override
  public ClassLoader loadResource(UniqueId driverId) {
    //assert (startupDriverId().equals(driverId));
    return this.getClass().getClassLoader();
  }

  @Override
  public void unloadFunctions(UniqueId driverId) {
    // never
    //assert (startupDriverId().equals(driverId));
  }
}