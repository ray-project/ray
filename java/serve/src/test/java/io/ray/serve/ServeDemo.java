package io.ray.serve;

import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Deployment;
import io.ray.serve.api.Serve;
import java.io.IOException;
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

public class ServeDemo {

  public static class Counter {

    private AtomicInteger count;

    public Counter(Integer value) {
      this.count = new AtomicInteger(value);
    }

    public Integer call(Integer delta) {
      return this.count.addAndGet(delta);
    }
  }

  public static void main(String[] args) throws IOException {
    Serve.start(true, true, null, null);
    Deployment deployment =
        Serve.deployment()
            .setName("counter")
            .setDeploymentDef(Counter.class.getName())
            .setNumReplicas(2)
            .setInitArgs(new Object[] {10});
    deployment.deploy(true);

    // Call deployment by handle.
    Assert.assertEquals(16, Ray.get(deployment.getHandle().remote(6)));

    // Make HTTP request to deployment.
    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
      HttpPut httpPut = new HttpPut("http://127.0.0.1/counter");
      httpPut.setEntity(
          new ByteArrayEntity(
              MessagePackSerializer.encode(new Object[] {3}).getKey(),
              ContentType.APPLICATION_OCTET_STREAM));
      try (CloseableHttpResponse response = httpclient.execute(httpPut)) {
        HttpEntity entity = response.getEntity();
        int result = MessagePackSerializer.decode(EntityUtils.toByteArray(entity), null);
        Assert.assertEquals(19, result);
      }
    }
  }
}
