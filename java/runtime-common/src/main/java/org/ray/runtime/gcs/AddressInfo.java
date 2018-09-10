package org.ray.runtime.gcs;

/**
 * Represents information of different process roles.
 */
public class AddressInfo {

  public String managerName;
  public String storeName;
  public String schedulerName;
  public String rayletSocketName;
  public int managerPort;
  public int workerCount;
  public String managerRpcAddr;
  public String storeRpcAddr;
  public String schedulerRpcAddr;
  public String rayletRpcAddr;
}
