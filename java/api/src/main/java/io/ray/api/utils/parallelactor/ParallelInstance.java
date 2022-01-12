package io.ray.api.utils.parallelactor;

import io.ray.api.call.ActorTaskCaller;

//// TODO: 这个不适合实现该接口，即使要实现，那么就需要在调task()的时候区分开是instance还是非instance.
//// 我们需要实现的是instance，然后，ParallelActor.task()则调的是某个instance.
public class ParallelInstance<A> implements ParallelActorCall<A> {

}
