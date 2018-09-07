package org.ray.runtime.functionmanager;

import com.google.common.base.Preconditions;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.id.UniqueId;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.util.Serializer;
import org.ray.runtime.util.logger.RayLog;

/**
 * local function manager which pulls remote functions on demand.
 */
public class LocalFunctionManager {

  private final RemoteFunctionManager remoteLoader;

  private final ConcurrentHashMap<UniqueId, FunctionTable> functionTables
      = new ConcurrentHashMap<>();

  /**
   * initialize load function manager using remote function manager to pull remote functions on
   * demand.
   */
  public LocalFunctionManager(RemoteFunctionManager remoteLoader) {
    this.remoteLoader = remoteLoader;
  }

  private FunctionTable loadDriverFunctions(UniqueId driverId) {
    FunctionTable functionTable = functionTables.get(driverId);
    if (functionTable == null) {
      RayLog.core.info("DriverId " + driverId + " Try to load functions");
      ClassLoader classLoader = remoteLoader.loadResource(driverId);
      if (classLoader == null) {
        throw new RuntimeException(
            "Cannot find resource' classLoader for app " + driverId.toString());
      }
      functionTable = new FunctionTable(classLoader);
      functionTables.put(driverId, functionTable);
    }
    return functionTable;
  }

  public Pair<ClassLoader, RayMethod> getMethod(UniqueId driverId, UniqueId actorId,
      UniqueId methodId, String className) {
    // assert the driver's resource is load.
    FunctionTable functionTable = loadDriverFunctions(driverId);
    Preconditions.checkNotNull(functionTable, "driver's resource is not loaded:%s", driverId);
    RayMethod method = actorId.isNil() ? functionTable.getTaskMethod(methodId, className)
        : functionTable.getActorMethod(methodId, className);
    Preconditions
        .checkNotNull(method, "method not found, class=%s, methodId=%s, driverId=%s", className,
            methodId, driverId);
    return Pair.of(functionTable.classLoader, method);
  }

  /**
   * get local method for executing, which pulls information from remote repo on-demand, therefore
   * it may block for a while if the related resources (e.g., jars) are not ready on local machine
   */
  public Pair<ClassLoader, RayMethod> getMethod(UniqueId driverId, UniqueId actorId,
      UniqueId methodId, FunctionArg[] args) {
    Preconditions.checkArgument(args.length >= 1, "method's args len %s<=1", args.length);
    String className = (String) Serializer.decode(args[args.length - 1].data);
    return getMethod(driverId, actorId, methodId, className);
  }

  /**
   * unload the functions when the driver is declared dead.
   */
  public synchronized void removeApp(UniqueId driverId) {
    FunctionTable funcs = functionTables.get(driverId);
    if (funcs != null) {
      functionTables.remove(driverId);
      remoteLoader.unloadFunctions(driverId);
    }
  }

  private static class FunctionTable {

    final ClassLoader classLoader;
    final ConcurrentHashMap<String, RayTaskMethods> taskMethods = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, RayActorMethods> actorMethods = new ConcurrentHashMap<>();

    FunctionTable(ClassLoader classLoader) {
      this.classLoader = classLoader;
    }

    RayMethod getTaskMethod(UniqueId methodId, String className) {
      RayTaskMethods taskMethods = this.taskMethods.get(className);
      if (taskMethods == null) {
        taskMethods = RayTaskMethods.fromClass(className, classLoader);
        RayLog.core.info("create RayTaskMethods: {}", taskMethods);
        this.taskMethods.put(className, taskMethods);
      }
      RayMethod m = taskMethods.functions.get(methodId);
      if (m != null) {
        return m;
      }
      // it is a actor static func.
      return getActorMethod(methodId, className, true);
    }

    RayMethod getActorMethod(UniqueId methodId, String className) {
      return getActorMethod(methodId, className, false);
    }

    private RayMethod getActorMethod(UniqueId methodId, String className, boolean isStatic) {
      RayActorMethods actorMethods = this.actorMethods.get(className);
      if (actorMethods == null) {
        actorMethods = RayActorMethods.fromClass(className, classLoader);
        RayLog.core.info("create RayActorMethods: {}", actorMethods);
        this.actorMethods.put(className, actorMethods);
      }
      return isStatic ? actorMethods.staticFunctions.get(methodId)
          : actorMethods.functions.get(methodId);
    }
  }
}
