package org.ray.spi.impl;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.ray.api.id.UniqueId;
import org.ray.format.gcs.ClientTableData;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.StateStoreProxy;
import org.ray.spi.model.AddressInfo;
import org.ray.util.NetworkUtil;
import org.ray.util.logger.RayLog;

/**
 * A class used to interface with the Ray control state.
 */
public class StateStoreProxyImpl implements StateStoreProxy {

  public KeyValueStoreLink rayKvStore;
  public ArrayList<KeyValueStoreLink> shardStoreList = new ArrayList<>();

  public StateStoreProxyImpl(KeyValueStoreLink rayKvStore) {
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
        return doGetAddressInfo(nodeIpAddress, redisAddress);
      } catch (Exception e) {
        try {
          RayLog.core.warn("Error occurred in StateStoreProxyImpl getAddressInfo, "
                  + (numRetries - count) + " retries remaining", e);
          TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException ie) {
          RayLog.core.error("error at StateStoreProxyImpl getAddressInfo", e);
          throw new RuntimeException(e);
        }
      }
      count++;
    }
    throw new RuntimeException("cannot get address info from state store");
  }

  /**
   * Get address info of one node from primary redis.
   * This method only tries to get address info once, without any retry.
   *
   * @param nodeIpAddress Usually local ip address.
   * @param redisAddress The primary redis address.
   * @return A list of SchedulerInfo which contains node manager or local scheduler address info.
   * @throws Exception No redis client exception.
   */
  public List<AddressInfo> doGetAddressInfo(final String nodeIpAddress,
                                            final String redisAddress) throws Exception {
    if (this.rayKvStore == null) {
      throw new Exception("no redis client when use doGetAddressInfo");
    }
    List<AddressInfo> schedulerInfo = new ArrayList<>();

    byte[] prefix = "CLIENT".getBytes();
    byte[] postfix = UniqueId.genNil().getBytes();
    byte[] clientKey = new byte[prefix.length + postfix.length];
    System.arraycopy(prefix, 0, clientKey, 0, prefix.length);
    System.arraycopy(postfix, 0, clientKey, prefix.length, postfix.length);

    Set<byte[]> clients = rayKvStore.zrange(clientKey, 0, -1);

    for (byte[] clientMessage : clients) {
      ByteBuffer bb = ByteBuffer.wrap(clientMessage);
      ClientTableData client = ClientTableData.getRootAsClientTableData(bb);
      String clientNodeIpAddress = client.nodeManagerAddress();

      String localIpAddress = NetworkUtil.getIpAddress(null);
      String redisIpAddress = redisAddress.substring(0, redisAddress.indexOf(':'));

      boolean headNodeAddress = "127.0.0.1".equals(clientNodeIpAddress)
              && Objects.equals(redisIpAddress, localIpAddress);
      boolean notHeadNodeAddress = Objects.equals(clientNodeIpAddress, nodeIpAddress);

      if (headNodeAddress || notHeadNodeAddress) {
        AddressInfo si = new AddressInfo();
        si.storeName = client.objectStoreSocketName();
        si.rayletSocketName = client.rayletSocketName();
        si.managerRpcAddr = client.nodeManagerAddress();
        si.managerPort = client.nodeManagerPort();
        schedulerInfo.add(si);
      }
    }
    return schedulerInfo;
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
