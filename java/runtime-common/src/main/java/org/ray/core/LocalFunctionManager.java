package org.ray.core;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.UniqueID;
import org.ray.spi.RemoteFunctionManager;
import org.ray.spi.model.FunctionArg;
import org.ray.spi.model.RayMethod;
import org.ray.util.MethodId;
import org.ray.util.logger.RayLog;

/**
 * local function manager which pulls remote functions on demand
 */
public class LocalFunctionManager {

  /**
   * initialize load function manager using remote function manager to pull remote functions on
   * demand
   */
  public LocalFunctionManager(RemoteFunctionManager remoteLoader) {
    this.remoteLoader = remoteLoader;
  }

  private synchronized FunctionTable loadDriverFunctions(UniqueID driverId) {
    FunctionTable functionTable = functionTables.get(driverId);
    if (null == functionTable) {
      RayLog.core.debug("DriverId " + driverId + " Try to load functions");
      LoadedFunctions funcs = remoteLoader.loadFunctions(driverId);
      if (funcs == null) {
        throw new RuntimeException("Cannot find resource for app " + driverId.toString());
      }
      functionTable = new FunctionTable(funcs);
      for (MethodId mid : functionTable.linkedFunctions.functions) {
        Method m = mid.load(funcs.loader);
        assert (m != null);
        RayMethod v = new RayMethod(m);
        v.check();
        UniqueID k = new UniqueID(mid.getSha1Hash());
        String logInfo =
            "DriverId" + driverId + " load remote function " + m.getName() + ", hash = " + k
                .toString();
        RayLog.core.debug(logInfo);
        functionTable.functions.put(k, v);
      }

      functionTables.put(driverId, functionTable);
    }
    // reSync automatically
    else {
      // more functions are loaded
      if (functionTable.linkedFunctions.functions.size() > functionTable.functions.size()) {
        for (MethodId mid : functionTable.linkedFunctions.functions) {
          UniqueID k = new UniqueID(mid.getSha1Hash());
          if (!functionTable.functions.containsKey(k)) {
            Method m = mid.load();
            assert (m != null);
            RayMethod v = new RayMethod(m);
            v.check();
            functionTable.functions.put(k, v);
          }
        }
      }
    }
    return functionTable;
  }

  /**
   * get local method for executing, which pulls information from remote repo on-demand, therefore
   * it may block for a while if the related resources (e.g., jars) are not ready on local machine
   */
  public Pair<ClassLoader, RayMethod> getMethod(UniqueID driverId, UniqueID methodId,
      FunctionArg[] args) throws NoSuchMethodException, SecurityException, ClassNotFoundException {
    FunctionTable funcs = loadDriverFunctions(driverId);
    final RayMethod m = funcs.functions.get(methodId);
    if (m == null) {
      throw new RuntimeException(
          "DriverId " + driverId + " load remote function methodId:" + methodId + " failed");
    }

    return Pair.of(funcs.linkedFunctions.loader, m);
  }

  /**
   * unload the functions when the driver is declared dead
   */
  public synchronized void removeApp(UniqueID driverId) {
    FunctionTable funcs = functionTables.get(driverId);
    if (funcs != null) {
      functionTables.remove(driverId);
      JarLoader.unloadJars(funcs.linkedFunctions.loader);
    }
  }

  private static class FunctionTable {

    final ConcurrentHashMap<UniqueID, RayMethod> functions = new ConcurrentHashMap<>();
    final LoadedFunctions linkedFunctions;

    FunctionTable(LoadedFunctions funcs) {
      this.linkedFunctions = funcs;
    }
  }

  private final RemoteFunctionManager remoteLoader;
  private final ConcurrentHashMap<UniqueID, FunctionTable> functionTables = new ConcurrentHashMap<>();
}
