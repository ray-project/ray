package org.ray.core;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.UniqueID;
import org.ray.hook.MethodId;
import org.ray.hook.runtime.JarLoader;
import org.ray.hook.runtime.LoadedFunctions;
import org.ray.spi.RemoteFunctionManager;
import org.ray.spi.model.FunctionArg;
import org.ray.spi.model.RayMethod;
import org.ray.util.logger.RayLog;

/**
 * local function manager which pulls remote functions on demand.
 */
public class LocalFunctionManager {

  private final RemoteFunctionManager remoteLoader;
  private final ConcurrentHashMap<UniqueID, FunctionTable> functionTables
      = new ConcurrentHashMap<>();

  /**
   * initialize load function manager using remote function manager to pull remote functions on
   * demand.
   */
  public LocalFunctionManager(RemoteFunctionManager remoteLoader) {
    this.remoteLoader = remoteLoader;
  }

  /**
   * get local method for executing, which pulls information from remote repo on-demand, therefore
   * it may block for a while if the related resources (e.g., jars) are not ready on local machine
   */
  public Pair<ClassLoader, RayMethod> getMethod(UniqueID driverId, UniqueID methodId,
                                                FunctionArg[] args) throws NoSuchMethodException,
      SecurityException, ClassNotFoundException {
    FunctionTable funcs = loadDriverFunctions(driverId);
    RayMethod m;

    // hooked methods
    if (!UniqueIdHelper.isLambdaFunction(methodId)) {
      m = funcs.functions.get(methodId);
      if (null == m) {
        throw new RuntimeException(
            "DriverId " + driverId + " load remote function methodId:" + methodId + " failed");
      }
    } else { // remote lambda
      assert args.length >= 2;
      String fname = Serializer.decode(args[args.length - 2].data);
      Method fm = Class.forName(fname).getMethod("execute", Object[].class);
      m = new RayMethod(fm);
    }

    return Pair.of(funcs.linkedFunctions.loader, m);
  }

  private synchronized FunctionTable loadDriverFunctions(UniqueID driverId) {
    FunctionTable funcs = functionTables.get(driverId);
    if (null == funcs) {
      RayLog.core.debug("DriverId " + driverId + " Try to load functions");
      LoadedFunctions funcs2 = remoteLoader.loadFunctions(driverId);
      if (funcs2 == null) {
        throw new RuntimeException("Cannot find resource for app " + driverId.toString());
      }
      funcs = new FunctionTable();
      funcs.linkedFunctions = funcs2;
      for (MethodId mid : funcs.linkedFunctions.functions) {
        Method m = mid.load();
        assert (m != null);
        RayMethod v = new RayMethod(m);
        v.check();
        UniqueID k = new UniqueID(mid.getSha1Hash());
        String logInfo =
            "DriverId" + driverId + " load remote function " + m.getName() + ", hash = " + k
                .toString();
        RayLog.core.debug(logInfo);
        System.err.println(logInfo);
        funcs.functions.put(k, v);
      }

      functionTables.put(driverId, funcs);
    } else { // reSync automatically
      // more functions are loaded
      if (funcs.linkedFunctions.functions.size() > funcs.functions.size()) {
        for (MethodId mid : funcs.linkedFunctions.functions) {
          UniqueID k = new UniqueID(mid.getSha1Hash());
          if (!funcs.functions.containsKey(k)) {
            Method m = mid.load();
            assert (m != null);
            RayMethod v = new RayMethod(m);
            v.check();
            funcs.functions.put(k, v);
          }
        }
      }
    }
    return funcs;
  }

  /**
   * unload the functions when the driver is declared dead.
   */
  public synchronized void removeApp(UniqueID driverId) {
    FunctionTable funcs = functionTables.get(driverId);
    if (funcs != null) {
      functionTables.remove(driverId);
      JarLoader.unloadJars(funcs.linkedFunctions.loader);
    }
  }

  static class FunctionTable {

    public final ConcurrentHashMap<UniqueID, RayMethod> functions = new ConcurrentHashMap<>();
    public LoadedFunctions linkedFunctions;
  }
}
