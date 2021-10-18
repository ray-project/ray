package io.ray.serve.util;

import com.google.protobuf.ByteString;
import io.ray.serve.BackendConfig;
import io.ray.serve.BackendVersion;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServeProtoUtilTest {

  @Test
  public void parseBackendConfigTest() {
    int numReplicas = 10;
    io.ray.serve.generated.BackendConfig pbBackendConfig =
        io.ray.serve.generated.BackendConfig.newBuilder().setNumReplicas(numReplicas).build();

    BackendConfig backendConfig = ServeProtoUtil.parseBackendConfig(pbBackendConfig.toByteArray());
    Assert.assertNotNull(backendConfig);
    Assert.assertEquals(backendConfig.getNumReplicas(), numReplicas);
    Assert.assertEquals(backendConfig.getBackendLanguage(), 0);
    Assert.assertEquals(backendConfig.getGracefulShutdownTimeoutS(), 20);
    Assert.assertEquals(backendConfig.getGracefulShutdownWaitLoopS(), 2);
    Assert.assertEquals(backendConfig.getMaxConcurrentQueries(), 100);
    Assert.assertNull(backendConfig.getUserConfig());
    Assert.assertEquals(backendConfig.isCrossLanguage(), false);
  }

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

  @Test
  public void parseBackendVersionTest() {
    String codeVersion = "parseBackendVersionTest";
    io.ray.serve.generated.BackendVersion pbBackendVersion =
        io.ray.serve.generated.BackendVersion.newBuilder().setCodeVersion(codeVersion).build();

    BackendVersion backendVersion =
        ServeProtoUtil.parseBackendVersion(pbBackendVersion.toByteArray());
    Assert.assertNotNull(backendVersion);
    Assert.assertEquals(backendVersion.getCodeVersion(), codeVersion);
  }

  @Test
  public void toBackendVersionProtobufTest() {
    String codeVersion = "toBackendVersionProtobufTest";
    BackendVersion backendVersion = new BackendVersion().setCodeVersion(codeVersion);
    io.ray.serve.generated.BackendVersion pbBackendVersion =
        ServeProtoUtil.toProtobuf(backendVersion);

    Assert.assertNotNull(pbBackendVersion);
    Assert.assertEquals(pbBackendVersion.getCodeVersion(), codeVersion);
  }
}
