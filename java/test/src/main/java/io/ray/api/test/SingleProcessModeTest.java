package io.ray.api.test;

import io.ray.api.Ray;
import io.ray.api.RayActor;
import io.ray.api.RayObject;
import io.ray.api.TestUtils;
import io.ray.api.id.ActorId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SingleProcessModeTest extends BaseTest {

  private static final int NUM_ACTOR_INSTANCE = 10;

  private static final int TIMES_TO_CALL_PER_ACTOR = 10;

  static class MyActor {
    public MyActor() {
    }

    public long getThreadId() {
      return Thread.currentThread().getId();
    }
  }

  @Test
  public void testActorTasksInOneThread() {
    TestUtils.skipTestUnderClusterMode();

    List<RayActor<MyActor>> actors = new ArrayList<>();
    Map<ActorId, Long> actorThreadIds = new HashMap<>();
    for (int i = 0; i < NUM_ACTOR_INSTANCE; ++i) {
      RayActor<MyActor> actor = Ray.createActor(MyActor::new);
      actors.add(actor);
      actorThreadIds.put(actor.getId(), actor.call(MyActor::getThreadId).get());
    }

    Map<ActorId, List<RayObject<Long>>> allResults = new HashMap<>();
    for (int i = 0; i < NUM_ACTOR_INSTANCE; ++i) {
      final RayActor<MyActor> actor = actors.get(i);
      List<RayObject<Long>> thisActorResult = new ArrayList<>();
      for (int j = 0; j < TIMES_TO_CALL_PER_ACTOR; ++j) {
        thisActorResult.add(actor.call(MyActor::getThreadId));
      }
      allResults.put(actor.getId(), thisActorResult);
    }

    // check result.
    for (int i = 0; i < NUM_ACTOR_INSTANCE; ++i) {
      final RayActor<MyActor> actor = actors.get(i);
      final List<RayObject<Long>> thisActorResult = allResults.get(actor.getId());
      // assert
      for (RayObject<Long> threadId : thisActorResult) {
        Assert.assertEquals(threadId.get(), actorThreadIds.get(actor.getId()));
      }
    }
  }
}
