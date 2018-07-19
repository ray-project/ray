package org.ray.spi.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.ray.api.UniqueID;
import org.ray.format.gcs.ClientTableData;
import org.ray.format.gcs.GcsTableEntry;
import org.ray.format.gcs.TablePrefix;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.StateStoreProxy;
import org.ray.spi.model.AddressInfo;
import org.ray.util.NetworkUtil;
import org.ray.util.logger.RayLog;
import redis.clients.util.SafeEncoder;

/**
 * A class used to interface with the Ray control state for raylet.
 */
public class RayletStateStoreProxyImpl extends StateStoreProxyImpl {

  public RayletStateStoreProxyImpl(KeyValueStoreLink rayKvStore) {
    super(rayKvStore);
  }

  @Override
  public synchronized void initializeGlobalState() throws Exception {

    String es;

    rayKvStore.checkConnected();

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
    if (ipAddressPorts.size() != numRedisShards) {
      es = String.format("Expected %d Redis shard addresses, found %d.", numRedisShards,
          ipAddressPorts.size());
      throw new Exception(es);
    }

    shardStoreList.clear();
    for (String ipPort : ipAddressPorts) {
      shardStoreList.add(new RedisClient(ipPort));
    }

  }

  /**
   *
   * Get address info of one node from primary redis.
   *
   * @param nodeIpAddress Usually local ip address.
   * @param redisAddress The primary redis address.
   * @return A list of SchedulerInfo which contains raylet name and raylet port.
   * @throws Exception No redis client exception.
   */
  @Override
  public List<AddressInfo> getAddressInfoHelper(final String nodeIpAddress,
      final String redisAddress) throws Exception {
    if (this.rayKvStore == null) {
      throw new Exception("no redis client when use getAddressInfoHelper");
    }
    List<AddressInfo> schedulerInfo = new ArrayList<>();

    byte[] prefix = "CLIENT".getBytes();
    byte[] postfix = UniqueID.genNil().getBytes();
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
        si.rayletName = client.rayletSocketName();
        si.managerRpcAddr = client.nodeManagerAddress();
        si.managerPort = client.nodeManagerPort();
        schedulerInfo.add(si);
      }
    }
    return schedulerInfo;
  }
}
