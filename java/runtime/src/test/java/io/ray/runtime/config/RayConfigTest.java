package io.ray.runtime.config;

import io.ray.runtime.generated.Common.WorkerType;
import io.ray.runtime.util.NetworkUtil;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayConfigTest {

  public static final int NUM_RETRIES = 5;

  @Test
  public void testCreateRayConfig() {
    try {
      System.setProperty("ray.job.resource-path", "path/to/ray/job/resource/path");
      RayConfig rayConfig = RayConfig.create();
      Assert.assertEquals(WorkerType.DRIVER, rayConfig.workerMode);
      Assert.assertEquals("path/to/ray/job/resource/path", rayConfig.jobResourcePath);
    } finally {
      // Unset system properties.
      System.clearProperty("ray.job.resource-path");
    }
  }

  @Test
  public void testGenerateHeadPortRandomly() {
    boolean isSame = true;
    final int port1 = RayConfig.create().headRedisPort;
    // If we the 2 ports are the same, let's retry.
    // This is used to avoid any flaky chance.
    for (int i = 0; i < NUM_RETRIES; ++i) {
      final int port2 = RayConfig.create().headRedisPort;
      if (port1 != port2) {
        isSame = false;
        break;
      }
    }
    Assert.assertFalse(isSame);
  }

  @Test
  public void testSpecifyHeadPort() {
    System.setProperty("ray.redis.head-port", "11111");
    Assert.assertEquals(RayConfig.create().headRedisPort, 11111);
  }

  // java -Dray.redis.address=10.15.239.80:6379 -Dray.raylet.node-manager-port=60349 -Dray.object-store.socket-name=/tmp/ray/session_2020-08-28_17-57-39_683660_47277/sockets/plasma_store -Dray.raylet.socket-name=/tmp/ray/session_2020-08-28_17-57-39_683660_47277/sockets/raylet -Dray.redis.password=5241590000000000 -Dray.home=/Users/chaokunyang/ant/Development/DevProjects/alibaba/ant_ray/ray/python/ray/../.. -Dray.logging.dir=/tmp/ray/session_2020-08-28_17-57-39_683660_47277/logs -Dray.session-dir=/tmp/ray/session_2020-08-28_17-57-39_683660_47277 -Dray.raylet.config.plasma_store_as_thread=True -Dray.raylet.config.num_workers_per_process_java=10 -classpath /Users/chaokunyang/ant/Development/DevProjects/alibaba/ant_ray/ray/python/ray/../../../build/java/*:/Users/chaokunyang/ant/Development/DevProjects/alibaba/ant_ray/ray/python/ray/jars/* io.ray.runtime.runner.worker.DefaultWorker

  @Test
  public void testNodeIp() {
    String ip = "8.8.8.8";
    int port = 53;
    try {
      Socket socket = new Socket(ip, port);
      System.out.println(socket.getLocalSocketAddress());
      System.out.println(socket.getLocalAddress().getHostAddress());
      System.out.println(socket.getInetAddress());
      System.out.println(InetAddress.getLocalHost().getHostName());
      System.out.println(InetAddress.getLocalHost().getHostAddress());
      System.out.println(NetworkUtil.getIpAddress(null));
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
