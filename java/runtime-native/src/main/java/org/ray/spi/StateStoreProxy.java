package org.ray.spi;

import java.util.List;
import java.util.Set;
import org.ray.spi.model.AddressInfo;

/**
 * Proxy client for state store, for instance redis.
 */
public interface StateStoreProxy {

  /**
   * setStore.
   * @param rayKvStore the underlying kv store used to store states
   */
  void setStore(KeyValueStoreLink rayKvStore);


  /**
   * initialize the store.
   */
  void initializeGlobalState() throws Exception;

  /**
   * keys.
   * @param pattern filter which keys you are interested in.
   */
  Set<String> keys(final String pattern);

  /**
   * getAddressInfo.
   * @return list of address information
   */
  List<AddressInfo> getAddressInfo(final String nodeIpAddress, 
                                   final String redisAddress,
                                   int numRetries);

  /**
   *
   * Get address info of one node from primary redis.
   *
   * @param nodeIpAddress Usually local ip address.
   * @param redisAddress The primary redis address.
   * @return A list of SchedulerInfo which contains raylet name and raylet port.
   * @throws Exception No redis client exception.
   */
  List<AddressInfo> getAddressInfoHelper(final String nodeIpAddress,
                      final String redisAddress) throws Exception;
}
