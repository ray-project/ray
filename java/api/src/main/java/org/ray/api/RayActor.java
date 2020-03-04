package org.ray.api;

import org.ray.api.id.ActorId;

/**
 * A handle to an actor. <p>
 *
 * A handle can be used to invoke a remote actor method, with the {@code "call"} method. For
 * example:
 * <pre> {@code
 * class MyActor {
 *   public int echo(int x) {
 *     return x;
 *   }
 * }
 * // Create an actor, and get a handle.
 * RayActor<MyActor> myActor = Ray.createActor(RayActor::new);
 * // Call the `echo` method remotely.
 * RayObject<Integer> result = myActor.call(MyActor::echo, 1);
 * // Get the result of the remote `echo` method.
 * Assert.assertEqual(result.get(), 1);
 * }</pre>
 *
 * Note, the {@code "call"} method is defined in {@link ActorCall} interface, with multiple
 * overloaded versions.
 *
 * @param <A> The type of the concrete actor class.
 */
public interface RayActor<A> extends ActorCall<A> {

  /**
   * @return The id of this actor.
   */
  ActorId getId();

  @Override
  default RayActor<A> getThis() {
    return this;
  }
}
