package org.ray.spi.impl;

import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.model.AddressInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class used to interface with the Ray control state for non-raylet.
 */
public class NonRayletStateStoreProxyImpl extends BaseStateStoreProxyImpl {
    public NonRayletStateStoreProxyImpl(KeyValueStoreLink rayKvStore) {
        super(rayKvStore);
    }

    /*
     * get address info of one node from primary redis
     * @param: node ip address, usually local ip address
     * @return: a list of SchedulerInfo which contains storeName, managerName, managerPort and
     * schedulerName
     * @note：Redis data key is "CL:*", redis data value is a hash.
     *        The hash contains the following：
     *        "deleted" : 0/1
     *        "ray_client_id"
     *        "node_ip_address"
     *        "client_type" : plasma_manager/local_scheduler
     *        "store_socket_name"(op)
     *        "manager_socket_name"(op)
     *        "local_scheduler_socket_name"(op)
     */
    @Override
    public List<AddressInfo> getAddressInfoHelper(final String nodeIpAddress,
                                                  final String redisAddress) throws Exception {
        if (this.rayKvStore == null) {
            throw new Exception("no redis client when use getAddressInfoHelper");
        }
        List<AddressInfo> schedulerInfo = new ArrayList<>();

        Set<byte[]> cks = rayKvStore.keys("CL:*".getBytes());
        byte[] key;
        List<Map<byte[], byte[]>> plasmaManager = new ArrayList<>();
        List<Map<byte[], byte[]>> localScheduler = new ArrayList<>();
        for (byte[] ck : cks) {
            key = ck;
            Map<byte[], byte[]> info = rayKvStore.hgetAll(key);

            String deleted = charsetDecode(info.get("deleted".getBytes()), "US-ASCII");
            if (deleted != null) {
                if (Boolean.getBoolean(deleted)) {
                    continue;
                }
            }

            if (!info.containsKey("ray_client_id".getBytes())) {
                throw new Exception("no ray_client_id in any client");
            } else if (!info.containsKey("node_ip_address".getBytes())) {
                throw new Exception("no node_ip_address in any client");
            } else if (!info.containsKey("client_type".getBytes())) {
                throw new Exception("no client_type in any client");
            }

            if (charsetDecode(info.get("node_ip_address".getBytes()), "US-ASCII")
                    .equals(nodeIpAddress)) {
                String clientType = charsetDecode(info.get("client_type".getBytes()), "US-ASCII");
                if ("plasma_manager".equals(clientType)) {
                    plasmaManager.add(info);
                } else if ("local_scheduler".equals(clientType)) {
                    localScheduler.add(info);
                }
            }
        }

        if (plasmaManager.size() < 1 || localScheduler.size() < 1) {
            throw new Exception("no plasma_manager or local_scheduler");
        } else if (plasmaManager.size() != localScheduler.size()) {
            throw new Exception("plasma_manager number not Equal local_scheduler number");
        }

        for (int i = 0; i < plasmaManager.size(); i++) {
            AddressInfo si = new AddressInfo();
            si.storeName = charsetDecode(plasmaManager.get(i).get("store_socket_name".getBytes()),
                    "US-ASCII");
            si.managerName = charsetDecode(plasmaManager.get(i).get("manager_socket_name".getBytes()),
                    "US-ASCII");

            byte[] rpc = plasmaManager.get(i).get("manager_rpc_name".getBytes());
            if (rpc != null) {
                si.managerRpcAddr = charsetDecode(rpc, "US-ASCII");
            }

            rpc = plasmaManager.get(i).get("store_rpc_name".getBytes());
            if (rpc != null) {
                si.storeRpcAddr = charsetDecode(rpc, "US-ASCII");
            }

            String managerAddr = charsetDecode(plasmaManager.get(i).get("manager_address".getBytes()),
                    "US-ASCII");
            si.managerPort = Integer.parseInt(managerAddr.split(":")[1]);
            si.schedulerName = charsetDecode(
                    localScheduler.get(i).get("local_scheduler_socket_name".getBytes()), "US-ASCII");

            rpc = localScheduler.get(i).get("local_scheduler_rpc_name".getBytes());
            if (rpc != null) {
                si.schedulerRpcAddr = charsetDecode(rpc, "US-ASCII");
            }

            schedulerInfo.add(si);
        }

        return schedulerInfo;
    }

}
