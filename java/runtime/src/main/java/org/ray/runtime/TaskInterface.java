package org.ray.runtime;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.id.ObjectId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.proxyTypes.ActorCreationOptionsProxy;
import org.ray.runtime.proxyTypes.RayFunctionProxy;
import org.ray.runtime.proxyTypes.TaskArgProxy;
import org.ray.runtime.proxyTypes.TaskOptionsProxy;
import org.ray.runtime.task.FunctionArg;

public class TaskInterface {
  private final long nativeCoreWorker;

  public TaskInterface(long nativeCoreWorker) {
    this.nativeCoreWorker = nativeCoreWorker;
  }

  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, FunctionArg[] args,
                                   int numReturns, CallOptions options) {
    RayFunctionProxy rayFunctionProxy = new RayFunctionProxy(functionDescriptor);
    List<TaskArgProxy> nativeArgs =
        Arrays.stream(args).map(TaskArgProxy::new).collect(Collectors.toList());
    TaskOptionsProxy taskOptionsProxy = new TaskOptionsProxy(numReturns, options);
    List<byte[]> returnIds = submitTask(nativeCoreWorker, rayFunctionProxy, nativeArgs,
        taskOptionsProxy);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  public RayActorImpl createActor(FunctionDescriptor functionDescriptor, FunctionArg[] args,
                                  ActorCreationOptions options) {
    RayFunctionProxy rayFunctionProxy = new RayFunctionProxy(functionDescriptor);
    List<TaskArgProxy> nativeArgs =
        Arrays.stream(args).map(TaskArgProxy::new).collect(Collectors.toList());
    ActorCreationOptionsProxy actorCreationOptionsProxy = new ActorCreationOptionsProxy(options);
    long nativeActorHandle = createActor(nativeCoreWorker,
        rayFunctionProxy, nativeArgs, actorCreationOptionsProxy);
    return new RayActorImpl(nativeActorHandle);
  }

  public List<ObjectId> submitActorTask(RayActorImpl actor, FunctionDescriptor functionDescriptor,
                                        FunctionArg[] args, int numReturns, CallOptions options) {
    RayFunctionProxy rayFunctionProxy = new RayFunctionProxy(functionDescriptor);
    List<TaskArgProxy> nativeArgs =
        Arrays.stream(args).map(TaskArgProxy::new).collect(Collectors.toList());
    TaskOptionsProxy taskOptionsProxy = new TaskOptionsProxy(numReturns, options);
    List<byte[]> returnIds = submitActorTask(nativeCoreWorker, actor.getNativeActorHandle(),
        rayFunctionProxy, nativeArgs, taskOptionsProxy);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  private static native List<byte[]> submitTask(long nativeCoreWorker,
                                                RayFunctionProxy rayFunction,
                                                List<TaskArgProxy> args,
                                                TaskOptionsProxy taskOptions);

  private static native long createActor(long nativeCoreWorker,
                                         RayFunctionProxy rayFunction,
                                         List<TaskArgProxy> args,
                                         ActorCreationOptionsProxy actorCreationOptions);

  private static native List<byte[]> submitActorTask(long nativeCoreWorker,
                                                     long nativeActorHandle,
                                                     RayFunctionProxy rayFunction,
                                                     List<TaskArgProxy> args,
                                                     TaskOptionsProxy taskOptions);
}
