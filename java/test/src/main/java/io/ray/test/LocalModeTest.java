package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LocalModeTest extends BaseTest {

  private static final int NUM_ACTOR_INSTANCE = 10;

  private static final int TIMES_TO_CALL_PER_ACTOR = 10;

  static class MyActor {
    public MyActor() {}

    public long getThreadId() {
      return Thread.currentThread().getId();
    }
  }

  @Test(groups = {"local"})
  public void testActorTasksInOneThread() {
    List<ActorHandle<MyActor>> actors = new ArrayList<>();
    Map<ActorId, Long> actorThreadIds = new HashMap<>();
    for (int i = 0; i < NUM_ACTOR_INSTANCE; ++i) {
      ActorHandle<MyActor> actor = Ray.actor(MyActor::new).remote();
      actors.add(actor);
      actorThreadIds.put(actor.getId(), actor.task(MyActor::getThreadId).remote().get());
    }

    Map<ActorId, List<ObjectRef<Long>>> allResults = new HashMap<>();
    for (int i = 0; i < NUM_ACTOR_INSTANCE; ++i) {
      final ActorHandle<MyActor> actor = actors.get(i);
      List<ObjectRef<Long>> thisActorResult = new ArrayList<>();
      for (int j = 0; j < TIMES_TO_CALL_PER_ACTOR; ++j) {
        thisActorResult.add(actor.task(MyActor::getThreadId).remote());
      }
      allResults.put(actor.getId(), thisActorResult);
    }

    // check result.
    for (int i = 0; i < NUM_ACTOR_INSTANCE; ++i) {
      final ActorHandle<MyActor> actor = actors.get(i);
      final List<ObjectRef<Long>> thisActorResult = allResults.get(actor.getId());
      // assert
      for (ObjectRef<Long> threadId : thisActorResult) {
        Assert.assertEquals(threadId.get(), actorThreadIds.get(actor.getId()));
      }
    }
  }
}
