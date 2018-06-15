package org.ray.spi.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.ray.api.UniqueID;
import org.ray.format.gcs.ClientTableData;
import org.ray.format.gcs.GcsTableEntry;
import org.ray.format.gcs.TablePrefix;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.StateStoreProxy;
import org.ray.spi.model.AddressInfo;
import org.ray.util.logger.RayLog;

/**
 * A class used to interface with the Ray control state for raylet.
 */
public class RayletStateStoreProxyImpl extends StateStoreProxyImpl {

  public RayletStateStoreProxyImpl(KeyValueStoreLink rayKvStore) {
    super(rayKvStore);
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

  /*
   * get address info of one node from primary redis
   * @param: node ip address, usually local ip address
   * @return: a list of SchedulerInfo which contains rayletName and rayletPort
   */
  public List<AddressInfo> getAddressInfoHelper(final String nodeIpAddress) throws Exception {
    if (this.rayKvStore == null) {
      throw new Exception("no redis client when use getAddressInfoHelper");
    }
    List<AddressInfo> schedulerInfo = new ArrayList<>();

    byte[] message = rayKvStore.sendCommand("RAY.TABLE_LOOKUP", 
          TablePrefix.CLIENT, UniqueID.genNil().getBytes());
    ByteBuffer byteBuffer = ByteBuffer.wrap(message);

    GcsTableEntry gcsEntry = GcsTableEntry.getRootAsGcsTableEntry(byteBuffer);
    int i = 0;
    while (i < gcsEntry.entriesLength()) {
      ByteBuffer byteBuffer2 = ByteBuffer.wrap(gcsEntry.entries(i).getBytes());
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
}
