package org.ray.spi.impl;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.StateStoreProxy;
import org.ray.spi.model.AddressInfo;
import org.ray.util.logger.RayLog;

/**
 * Base class used to interface with the Ray control state.
 */
public abstract class BaseStateStoreProxyImpl implements StateStoreProxy {

  public KeyValueStoreLink rayKvStore;
  public ArrayList<KeyValueStoreLink> shardStoreList = new ArrayList<>();

  public BaseStateStoreProxyImpl(KeyValueStoreLink rayKvStore) {
    this.rayKvStore = rayKvStore;
  }

  @Override
  public void setStore(KeyValueStoreLink rayKvStore) {
    this.rayKvStore = rayKvStore;
  }

  @Override
  public synchronized void initializeGlobalState() throws Exception {

    String es;

    checkConnected();

    String s = rayKvStore.get("NumRedisShards", null);
    if (s == null) {
      throw new Exception("NumRedisShards not found in redis.");
    }
    int numRedisShards = Integer.parseInt(s);
    if (numRedisShards < 1) {
      es = String.format("Expected at least one Redis shard, found %d", numRedisShards);
      throw new Exception(es);
    }
    List<String> ipAddressPorts = rayKvStore.lrange("RedisShards", 0, -1);
    Set<String> distinctIpAddress = new HashSet<String>(ipAddressPorts);
    if (distinctIpAddress.size() != numRedisShards) {
      es = String.format("Expected %d Redis shard addresses, found2 %d.", numRedisShards,
        distinctIpAddress.size());
      throw new Exception(es);
    }

    shardStoreList.clear();
    for (String ipPort : distinctIpAddress) {
      shardStoreList.add(new RedisClient(ipPort));
    }

  }

  public void checkConnected() throws Exception {
    rayKvStore.checkConnected();
  }

  @Override
  public synchronized Set<String> keys(final String pattern) {
    Set<String> allKeys = new HashSet<>();
    Set<String> tmpKey;
    for (KeyValueStoreLink ashardStoreList : shardStoreList) {
      tmpKey = ashardStoreList.keys(pattern);
      allKeys.addAll(tmpKey);
    }

    return allKeys;
  }

  @Override
  public List<AddressInfo> getAddressInfo(final String nodeIpAddress,
                                          final String redisAddress, 
                                          int numRetries) {
    int count = 0;
    while (count < numRetries) {
      try {
        return getAddressInfoHelper(nodeIpAddress, redisAddress);
      } catch (Exception e) {
        try {
          RayLog.core.warn("Error occurred in BaseStateStoreProxyImpl getAddressInfo, "
              + (numRetries - count) + " retries remaining", e);
          TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException ie) {
          RayLog.core.error("error at BaseStateStoreProxyImpl getAddressInfo", e);
          throw new RuntimeException(e);
        }
      }
      count++;
    }
    throw new RuntimeException("cannot get address info from state store");
  }

  protected String charsetDecode(byte[] bs, String charset) throws UnsupportedEncodingException {
    return new String(bs, charset);
  }

  protected byte[] charsetEncode(String str, String charset) throws UnsupportedEncodingException {
    if (str != null) {
      return str.getBytes(charset);
    }
    return null;
  }
}
