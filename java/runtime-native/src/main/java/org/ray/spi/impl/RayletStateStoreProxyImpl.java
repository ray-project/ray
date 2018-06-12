package org.ray.spi.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
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
import org.ray.api.UniqueID;
import org.ray.util.logger.RayLog;
import org.ray.format.gcs.GcsTableEntry;
import org.ray.format.gcs.ClientTableData;
import org.ray.format.gcs.TablePrefix;
import com.google.flatbuffers.*;

/**
 * A class used to interface with the Ray control state
 */
public class RayletStateStoreProxyImpl implements StateStoreProxy {

  public KeyValueStoreLink rayKvStore;
  public ArrayList<KeyValueStoreLink> shardStoreList = new ArrayList<>();

  public RayletStateStoreProxyImpl(KeyValueStoreLink rayKvStore) {
    this.rayKvStore = rayKvStore;
  }

  public void setStore(KeyValueStoreLink rayKvStore) {
    this.rayKvStore = rayKvStore;
  }

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

  public synchronized Set<String> keys(final String pattern) {
    Set<String> allKeys = new HashSet<>();
    Set<String> tmpKey;
    for (KeyValueStoreLink a_shardStoreList : shardStoreList) {
      tmpKey = a_shardStoreList.keys(pattern);
      allKeys.addAll(tmpKey);
    }

    return allKeys;
  }

  private byte[] CharsetEncode(String str, String Charset) throws UnsupportedEncodingException {
    if (str != null) {
      return str.getBytes(Charset);
    }
    return null;
  }

  private String CharsetDecode(byte[] bs, String Charset) throws UnsupportedEncodingException {
    return new String(bs, Charset);
  }

  /*
   * get address info of one node from primary redis
   * @param: node ip address, usually local ip address
   * @return: a list of SchedulerInfo which contains storeName, managerName, managerPort and schedulerName
   * @note：Redis data key is "CL:*", redis data value is a hash.
   *        The hash contains the following：
   *        "deleted" : 0/1
   *        "ray_client_id"
   *        "nodeIpAddress"
   *        "client_type" : plasma_manager/local_scheduler
   *        "store_socket_name"(op)
   *        "manager_socket_name"(op)
   *        "local_scheduler_socket_name"(op)
   */
  public List<AddressInfo> getAddressInfoHelper(final String nodeIpAddress) throws Exception {
    if (this.rayKvStore == null) {
      throw new Exception("no redis client when use getAddressInfoHelper");
    }
    List<AddressInfo> schedulerInfo = new ArrayList<>();

    byte[] message = rayKvStore.SendCommand( "RAY.TABLE_LOOKUP", 
          TablePrefix.CLIENT, UniqueID.genNil().getBytes());
    ByteBuffer byteBuffer = ByteBuffer.wrap(message);

    GcsTableEntry gcs_entry = GcsTableEntry.getRootAsGcsTableEntry(byteBuffer);
    int i = 0;
    while ( i < gcs_entry.entriesLength()) {
      ByteBuffer byteBuffer2 = ByteBuffer.wrap(gcs_entry.entries(i).getBytes());
      ClientTableData client = ClientTableData.getRootAsClientTableData(byteBuffer2);

      AddressInfo si = new AddressInfo();
      si.storeName = client.objectStoreSocketName();
      si.rayletName = client.rayletSocketName();
      si.managerRpcAddr = client.nodeManagerAddress();
      si.managerPort = client.nodeManagerPort();
      schedulerInfo.add(si);
    }
    return schedulerInfo;
  }

  public List<AddressInfo> getAddressInfo(final String node_ip_address, int numRetries) {
    int count = 0;
    while (count < numRetries) {
      try {
        return getAddressInfoHelper(node_ip_address);
      } catch (Exception e) {
        try {
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
}
