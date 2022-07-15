package io.ray.serve.util;

import com.google.protobuf.ByteString;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServeProtoUtilTest {

  @Test
  public void parseRequestMetadataTest() {
    String prefix = "parseRequestMetadataTest";
    String requestId = RandomStringUtils.randomAlphabetic(10);
    String callMethod = prefix + "_method";
    String endpoint = prefix + "_endpoint";
    String context = prefix + "_context";
    RequestMetadata requestMetadata =
        RequestMetadata.newBuilder()
            .setRequestId(requestId)
            .setCallMethod(callMethod)
            .setEndpoint(endpoint)
            .putContext("context", context)
            .build();

    RequestMetadata result = ServeProtoUtil.parseRequestMetadata(requestMetadata.toByteArray());
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getCallMethod(), callMethod);
    Assert.assertEquals(result.getEndpoint(), endpoint);
    Assert.assertEquals(result.getRequestId(), requestId);
    Assert.assertEquals(result.getContextMap().get("context"), context);
  }

  @Test
  public void parseRequestWrapperTest() {
    byte[] body = new byte[] {1, 2};
    RequestWrapper requestWrapper =
        RequestWrapper.newBuilder().setBody(ByteString.copyFrom(body)).build();

    RequestWrapper result = ServeProtoUtil.parseRequestWrapper(requestWrapper.toByteArray());
    Assert.assertNotNull(result);
    byte[] rstBody = result.getBody().toByteArray();
    Assert.assertEquals(rstBody[0], 1);
    Assert.assertEquals(rstBody[1], 2);
  }
}
