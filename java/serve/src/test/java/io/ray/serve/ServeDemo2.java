package io.ray.serve;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Assert;

import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;

public class ServeDemo2 {

  public static class Counter {

    private AtomicInteger count;

    public Counter(Integer value) {
      this.count = new AtomicInteger(value);
    }

    public HttpResponse call(HttpRequest request) {
      byte[] content = request.getContent();
      int result = this.count.addAndGet(MessagePackSerializer.decode(content, Integer.class));
      return new HttpResponse().setContent(MessagePackSerializer.encode(result).getKey());
    }
  }

  public static void main(String[] args) throws IOException {
    Map<String, String> proxyConfig = new HashMap<>();
    proxyConfig.put("ray.serve.proxy.http.raw", "true");
    Serve.start(true, true, null, null, null);
    Deployment deployment =
        Serve.deployment()
            .setName("counter")
            .setDeploymentDef(Counter.class.getName())
            .setNumReplicas(2)
            .setInitArgs(new Object[] {10});
    deployment.deploy(true);

    // Make HTTP request to deployment.
    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
      HttpPut httpPut = new HttpPut("http://127.0.0.1/counter");
      httpPut.setEntity(
          new ByteArrayEntity(
              MessagePackSerializer.encode(3).getKey(), ContentType.APPLICATION_OCTET_STREAM));
      try (CloseableHttpResponse response = httpclient.execute(httpPut)) {
        HttpEntity entity = response.getEntity();
        int result = MessagePackSerializer.decode(EntityUtils.toByteArray(entity), Integer.class);
        Assert.assertEquals(13, result);
      }
    }
  }
}
