package org.ray.lib.actorgroup.api;

import java.util.List;
import org.ray.api.RayActor;
import org.ray.lib.actorgroup.api.runtime.RayActorGroupRuntime;

public class RayActorGroup extends RayActorGroupCall {

  private static RayActorGroupRuntime rayActorGroupRuntime = null;

  public static void init() {
    //TODO(yuyiming): implement it
  }

  public static <T> ActorGroup<T> createActorGroup(String name, List<RayActor<T>> actors) {
    return rayActorGroupRuntime.createActorGroup(name, actors);
  }

  public static <T> ActorGroup<T> getActorGroup(String name) {
    return rayActorGroupRuntime.getActorGroup(name);
  }

  public static RayActorGroupRuntime internal() {
    return rayActorGroupRuntime;
  }
}
