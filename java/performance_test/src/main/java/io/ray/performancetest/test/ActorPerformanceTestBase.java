package io.ray.performancetest.test;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.performancetest.Receiver;
import io.ray.performancetest.Source;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActorPerformanceTestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActorPerformanceTestBase.class);

  public static void run(
      String[] args,
      int[] layers,
      int[] actorsPerLayer,
      boolean hasReturn,
      boolean ignoreReturn,
      int argSize,
      boolean useDirectByteBuffer) {
    System.setProperty("ray.raylet.startup-token", "0");
    Ray.init();
    try {
      // TODO: Support more layers.
      Preconditions.checkState(layers.length == 2);
      Preconditions.checkState(actorsPerLayer.length == layers.length);
      for (int i = 0; i < layers.length; i++) {
        Preconditions.checkState(layers[i] > 0);
        Preconditions.checkState(actorsPerLayer[i] > 0);
      }

      List<ActorHandle<Receiver>> receivers = new ArrayList<>();
      for (int i = 0; i < layers[1]; i++) {
        int nodeIndex = layers[0] + i;
        for (int j = 0; j < actorsPerLayer[1]; j++) {
          receivers.add(Ray.actor(Receiver::new).remote());
        }
      }

      List<ActorHandle<Source>> sources = new ArrayList<>();
      for (int i = 0; i < layers[0]; i++) {
        int nodeIndex = i;
        for (int j = 0; j < actorsPerLayer[0]; j++) {
          sources.add(Ray.actor(Source::new, receivers).remote());
        }
      }

      List<ObjectRef<Boolean>> results =
          sources.stream()
              .map(
                  source ->
                      source
                          .task(
                              Source::startTest,
                              hasReturn,
                              ignoreReturn,
                              argSize,
                              useDirectByteBuffer)
                          .remote())
              .collect(Collectors.toList());

      Ray.get(results);
    } catch (Throwable e) {
      LOGGER.error("Run test failed.", e);
      throw e;
    }
  }
}
