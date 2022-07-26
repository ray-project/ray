package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.handle.RayServeHandle;
import io.ray.serve.proxy.ProxyRouter;
import io.ray.serve.util.CommonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProxyRouterTest extends BaseTest {
  @Test
  public void test() {
    init();
    try {
      String prefix = "ProxyRouterTest";
      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      String endpointName1 = prefix + "_1";
      String endpointName2 = prefix + "_2";
      String route1 = "/route1";
      Map<String, String> config = new HashMap<>();
      config.put(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false");

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new, "").setName(controllerName).remote();
      Map<String, EndpointInfo> endpointInfos = new HashMap<>();
      endpointInfos.put(
          endpointName1,
          EndpointInfo.newBuilder().setEndpointName(endpointName1).setRoute(route1).build());
      endpointInfos.put(
          endpointName2, EndpointInfo.newBuilder().setEndpointName(endpointName2).build());
      controllerHandle.task(DummyServeController::setEndpoints, endpointInfos).remote();

      Serve.setInternalReplicaContext(null, null, controllerName, null, config);

      // ProxyRouter updates routes.
      ProxyRouter proxyRouter = new ProxyRouter();
      proxyRouter.updateRoutes(endpointInfos);

      // Check result.
      Map<String, EndpointInfo> routeInfo = proxyRouter.getRouteInfo();
      Assert.assertNotNull(routeInfo);
      Assert.assertNotNull(routeInfo.get(route1));
      Assert.assertEquals(routeInfo.get(route1).getRoute(), route1);
      Assert.assertEquals(routeInfo.get(route1).getEndpointName(), endpointName1);
      Assert.assertNotNull(routeInfo.get(endpointName2));
      Assert.assertEquals(routeInfo.get(endpointName2).getEndpointName(), endpointName2);
      Map<String, RayServeHandle> handles = proxyRouter.getHandles();
      Assert.assertNotNull(handles);
      Assert.assertNotNull(handles.get(endpointName1));
      Assert.assertNotNull(handles.get(endpointName2));
    } finally {
      shutdown();
    }
  }
}
