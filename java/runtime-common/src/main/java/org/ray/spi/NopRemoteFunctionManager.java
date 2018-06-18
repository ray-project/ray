package org.ray.spi;

import org.ray.api.UniqueID;

/**
 * mock version of remote function manager using local loaded jars.
 */
public class NopRemoteFunctionManager implements RemoteFunctionManager {

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
  public ClassLoader loadResource(UniqueID driverId) {
    //assert (startupDriverId().equals(driverId));
    return this.getClass().getClassLoader();
  }

  @Override
  public void unloadFunctions(UniqueID driverId) {
    // never
    //assert (startupDriverId().equals(driverId));
  }
}