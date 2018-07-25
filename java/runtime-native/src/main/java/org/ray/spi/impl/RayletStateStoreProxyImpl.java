package org.ray.spi.impl;

import java.nio.ByteBuffer;
import java.util.*;

import org.ray.api.UniqueID;
import org.ray.format.gcs.ClientTableData;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.model.AddressInfo;
import org.ray.util.NetworkUtil;

/**
 * A class used to interface with the Ray control state for raylet.
 */
public class RayletStateStoreProxyImpl extends BaseStateStoreProxyImpl {

  public RayletStateStoreProxyImpl(KeyValueStoreLink rayKvStore) {
    super(rayKvStore);
  }

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
        si.rayletSocketName = client.rayletSocketName();
        si.managerRpcAddr = client.nodeManagerAddress();
        si.managerPort = client.nodeManagerPort();
        schedulerInfo.add(si);
      }
    }
    return schedulerInfo;
  }
}
