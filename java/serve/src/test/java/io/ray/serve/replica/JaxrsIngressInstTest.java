package io.ray.serve.replica;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.ray.serve.generated.RequestWrapper;
import io.ray.serve.util.ExampleJaxrsDeployment;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JaxrsIngressInstTest {
  /** Method: call(RequestWrapper httpProxyRequest) */
  @Test
  public void testCallQueryParam() throws Exception {
    JaxrsIngressInst ingressInst = new JaxrsIngressInst(ExampleJaxrsDeployment.class);
    RequestWrapper requestWrapper =
        RequestWrapper.newBuilder()
            .putAllScope(
                ImmutableMap.of(
                    "method", "GET", "path", "/example/helloWorld", "query_string", "name=ray"))
            .build();
    Object obj = ingressInst.call(requestWrapper);
    Assert.assertEquals(obj, "hello world, ray");
  }

  @Test
  public void testCallPathParam() throws Exception {
    JaxrsIngressInst ingressInst = new JaxrsIngressInst(ExampleJaxrsDeployment.class);
    RequestWrapper requestWrapper =
        RequestWrapper.newBuilder()
            .putAllScope(ImmutableMap.of("method", "GET", "path", "/example/paramPathTest/ray"))
            .build();
    Object obj = ingressInst.call(requestWrapper);
    Assert.assertEquals(obj, "paramPathTest, ray");
  }

  @Test
  public void testCallBody() throws Exception {
    JaxrsIngressInst ingressInst = new JaxrsIngressInst(ExampleJaxrsDeployment.class);
    RequestWrapper requestWrapper =
        RequestWrapper.newBuilder()
            .putAllScope(ImmutableMap.of("method", "POST", "path", "/example/helloWorld2"))
            .setBody(ByteString.copyFrom("ray".getBytes(StandardCharsets.UTF_8)))
            .build();
    Object obj = ingressInst.call(requestWrapper);
    Assert.assertEquals(obj, "hello world, i am from body:ray");
  }
}
