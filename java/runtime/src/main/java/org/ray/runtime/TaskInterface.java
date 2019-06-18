package org.ray.runtime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.ray.api.id.ObjectId;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.nativeTypes.NativeRayFunction;
import org.ray.runtime.nativeTypes.NativeTaskArg;
import org.ray.runtime.nativeTypes.NativeTaskOptions;
import org.ray.runtime.task.FunctionArg;

public class TaskInterface {
  private final long nativeCoreWorker;

  public TaskInterface(long nativeCoreWorker) {
    this.nativeCoreWorker = nativeCoreWorker;
  }

  public List<ObjectId> submitTask(FunctionDescriptor functionDescriptor, FunctionArg[] args,
                                   int numReturns, Map<String, Double> resources) {
    NativeRayFunction nativeRayFunction = new NativeRayFunction(functionDescriptor);
    List<NativeTaskArg> nativeArgs =
        Arrays.stream(args).map(NativeTaskArg::new).collect(Collectors.toList());
    if (resources == null) {
      resources = new HashMap<>();
    }
    NativeTaskOptions nativeTaskOptions = new NativeTaskOptions(numReturns, resources);
    List<byte[]> returnIds = submitTask(nativeCoreWorker, nativeRayFunction, nativeArgs,
        nativeTaskOptions);
    return returnIds.stream().map(ObjectId::new).collect(Collectors.toList());
  }

  private static native List<byte[]> submitTask(long nativeCoreWorker,
                                                  NativeRayFunction rayFunction,
                                                  List<NativeTaskArg> args,
                                                  NativeTaskOptions taskOptions);
}
