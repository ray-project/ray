package org.ray.runtime.task;

import java.util.List;
import org.ray.api.BaseActor;
import org.ray.api.id.ObjectId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.runtime.functionmanager.FunctionDescriptor;

/**
 * A set of methods to submit tasks and create actors.
 */
public interface TaskSubmitter {

  /**
   * Submit a normal task.
   * @param functionDescriptor The remote function to execute.
   * @param args Arguments of this task.
   * @param numReturns Return object count.
   * @param options Options for this task.
   * @return Ids of the return objects.
   */
  List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      int numReturns, CallOptions options);

  /**
   * Create an actor.
   * @param functionDescriptor The remote function that generates the actor object.
   * @param args Arguments of this task.
   * @param options Options for this actor creation task.
   * @return Handle to the actor.
   */
  BaseActor createActor(FunctionDescriptor functionDescriptor, List<FunctionArg> args,
      ActorCreationOptions options);

  /**
   * Submit an actor task.
   * @param actor Handle to the actor.
   * @param functionDescriptor The remote function to execute.
   * @param args Arguments of this task.
   * @param numReturns Return object count.
   * @param options Options for this task.
   * @return Ids of the return objects.
   */
  List<ObjectId> submitActorTask(BaseActor actor, FunctionDescriptor functionDescriptor,
      List<FunctionArg> args, int numReturns, CallOptions options);
}
