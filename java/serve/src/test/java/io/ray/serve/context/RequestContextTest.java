package io.ray.serve.context;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RequestContextTest {

  @Test
  public void test() {
    RequestContext context = RequestContext.get();
    Assert.assertEquals(context.getRoute(), "");
    Assert.assertEquals(context.getRequestId(), "");
    Assert.assertEquals(context.getAppName(), "");
    Assert.assertEquals(context.getMultiplexedModelId(), "");

    String route = "route";
    String requestId = "requestId";
    String appName = "appName";
    String multiplexedModelId = "multiplexedModelId";

    RequestContext.set(route, requestId, appName, multiplexedModelId);
    context = RequestContext.get();
    Assert.assertEquals(context.getRoute(), route);
    Assert.assertEquals(context.getRequestId(), requestId);
    Assert.assertEquals(context.getAppName(), appName);
    Assert.assertEquals(context.getMultiplexedModelId(), multiplexedModelId);

    RequestContext.clean();
    context = RequestContext.get();
    Assert.assertEquals(context.getRoute(), "");
    Assert.assertEquals(context.getRequestId(), "");
    Assert.assertEquals(context.getAppName(), "");
    Assert.assertEquals(context.getMultiplexedModelId(), "");
    RequestContext.clean();
  }
}
