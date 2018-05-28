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
 * A class used to interface with the Ray control state
 */
public class StateStoreProxyImpl implements StateStoreProxy {

  public KeyValueStoreLink rayKvStore;
  public ArrayList<KeyValueStoreLink> shardStoreList = new ArrayList<>();

  public StateStoreProxyImpl(KeyValueStoreLink rayKvStore) {
    this.rayKvStore = rayKvStore;
  }

  public void setStore(KeyValueStoreLink rayKvStore) {
    this.rayKvStore = rayKvStore;
  }

  public void checkConnected() throws Exception {
    rayKvStore.CheckConnected();
  }

  public synchronized void initializeGlobalState() throws Exception {

    String es;

    checkConnected();

    String s = rayKvStore.Get("NumRedisShards", null);
    if (s == null) {
      throw new Exception("NumRedisShards not found in redis.");
    }
    int numRedisShards = Integer.parseInt(s);
    if (numRedisShards < 1) {
      es = String.format("Expected at least one Redis shard, found %d", numRedisShards);
      throw new Exception(es);
    }
    List<String> ipAddressPorts = rayKvStore.Lrange("RedisShards", 0, -1);
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
      tmpKey = a_shardStoreList.Keys(pattern);
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

    Set<byte[]> cks = rayKvStore.Keys("CL:*".getBytes());
    byte[] key;
    List<Map<byte[], byte[]>> plasmaManager = new ArrayList<>();
    List<Map<byte[], byte[]>> localScheduler = new ArrayList<>();
    for (byte[] ck : cks) {
      key = ck;
      Map<byte[], byte[]> info = rayKvStore.hgetAll(key);

      String deleted = CharsetDecode(info.get("deleted".getBytes()), "US-ASCII");
      if (deleted != null) {
        if (Boolean.getBoolean(deleted)) {
          continue;
        }
      }

      if (!info.containsKey("ray_client_id".getBytes())) {
        throw new Exception("no ray_client_id in any client");
      } else if (!info.containsKey("nodeIpAddress".getBytes())) {
        throw new Exception("no nodeIpAddress in any client");
      } else if (!info.containsKey("client_type".getBytes())) {
        throw new Exception("no client_type in any client");
      }

      if (CharsetDecode(info.get("nodeIpAddress".getBytes()), "US-ASCII")
          .equals(nodeIpAddress)) {
        String clientType = CharsetDecode(info.get("client_type".getBytes()), "US-ASCII");
        if (clientType.equals("plasmaManager")) {
          plasmaManager.add(info);
        } else if (clientType.equals("localScheduler")) {
          localScheduler.add(info);
        }
      }
    }

    if (plasmaManager.size() < 1 || localScheduler.size() < 1) {
      throw new Exception("no plasmaManager or localScheduler");
    } else if (plasmaManager.size() != localScheduler.size()) {
      throw new Exception("plasmaManager number not Equal localScheduler number");
    }

    for (int i = 0; i < plasmaManager.size(); i++) {
      AddressInfo si = new AddressInfo();
      si.storeName = CharsetDecode(plasmaManager.get(i).get("store_socket_name".getBytes()),
          "US-ASCII");
      si.managerName = CharsetDecode(plasmaManager.get(i).get("manager_socket_name".getBytes()),
          "US-ASCII");

      byte[] rpc = plasmaManager.get(i).get("manager_rpc_name".getBytes());
      if (rpc != null) {
        si.managerRpcAddr = CharsetDecode(rpc, "US-ASCII");
      }

      rpc = plasmaManager.get(i).get("store_rpc_name".getBytes());
      if (rpc != null) {
        si.storeRpcAddr = CharsetDecode(rpc, "US-ASCII");
      }

      String managerAddr = CharsetDecode(plasmaManager.get(i).get("manager_address".getBytes()),
          "US-ASCII");
      si.managerPort = Integer.parseInt(managerAddr.split(":")[1]);
      si.schedulerName = CharsetDecode(
          localScheduler.get(i).get("local_scheduler_socket_name".getBytes()), "US-ASCII");

      rpc = localScheduler.get(i).get("local_scheduler_rpc_name".getBytes());
      if (rpc != null) {
        si.schedulerRpcAddr = CharsetDecode(rpc, "US-ASCII");
      }

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
