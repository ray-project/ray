package org.ray.util;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.WeakHashMap;
import org.objectweb.asm.Type;
import org.ray.util.logger.RayLog;


/**
 * An instance of RayFunc is a lambda.
 * MethodId describe the information of the called function in lambda.<br/>
 * e.g. Ray.call(Foo::foo), the MethodId of the lambda Foo::foo is:<br/>
 * MethodId.className = Foo <br/>
 * MethodId.methodName = foo <br/>
 * MethodId.methodDesc = describe the types of args and return.
 * see org.objectweb.asm.Type.getDescriptor.
 */
public final class MethodId {

  /**
   * use ThreadLocal to avoid lock.
   * A cache from the lambda instances to MethodId.
   * Note: the lambda instances are dynamically created per call site,
   * we use WeakHashMap to avoid OOM.
   */
  private static final ThreadLocal<WeakHashMap<Class<Serializable>, MethodId>>
      CACHE = ThreadLocal.withInitial(() -> new WeakHashMap<>());

  public final String className;
  public final String methodName;

  public final String methodDesc;
  public final boolean isStatic;
  /**
   * encode the className,methodName,methodDesc,isStatic as an uniquel id.
   */
  private final String encoding;

  /**
   * sha1 from the encoding, used as functionId.
   */
  private final byte[] digest;

  public MethodId(String className, String methodName, String methodDesc, boolean isStatic) {
    this.className = className;
    this.methodName = methodName;
    this.methodDesc = methodDesc;
    this.isStatic = isStatic;
    this.encoding = encode(className, methodName, methodDesc, isStatic);
    this.digest = getSha1Hash0();
  }

  private static String encode(String className, String methodName, String methodDesc,
      boolean isStatic) {
    StringBuilder sb = new StringBuilder(512);
    sb.append(className).append('/').append(methodName).append("::").append(methodDesc).append("&&")
        .append(isStatic);
    return sb.toString();
  }

  public static MethodId fromExecutable(Executable method) {
    final boolean isStatic = Modifier.isStatic(method.getModifiers());
    final String className = method.getDeclaringClass().getName();
    final String methodName = method instanceof Method
      ? method.getName() : "<init>";
    final Type type = method instanceof Method
        ? Type.getType((Method) method) : Type.getType((Constructor) method);
    final String methodDesc = type.getDescriptor();
    return new MethodId(className, methodName, methodDesc, isStatic);
  }

  public static MethodId fromSerializedLambda(Serializable serial) {
    return fromSerializedLambda(serial, false);
  }

  public static MethodId fromSerializedLambda(Serializable serial, boolean forceNew) {
    Preconditions.checkArgument(!(serial instanceof SerializedLambda), "arg could not be "
        + "SerializedLambda");
    Class<Serializable> clazz = (Class<Serializable>) serial.getClass();
    WeakHashMap<Class<Serializable>, MethodId> map = CACHE.get();
    MethodId id = map.get(clazz);
    if (id == null || forceNew) {
      final SerializedLambda lambda = LambdaUtils.getSerializedLambda(serial);
      Preconditions.checkArgument(lambda.getCapturedArgCount() == 0, "could not transfer a lambda "
          + "which is closure");
      final boolean isStatic = lambda.getImplMethodKind() == MethodHandleInfo.REF_invokeStatic;
      final String className = lambda.getImplClass().replace('/', '.');
      id = new MethodId(className, lambda.getImplMethodName(),
          lambda.getImplMethodSignature(), isStatic);
      if (!forceNew) {
        map.put(clazz, id);
      }
    }
    return id;
  }

  public Method load() {
    return load(null);
  }

  public Method load(ClassLoader loader) {
    Class<?> cls = null;
    try {
      RayLog.core.debug(
          "load class " + className + " from class loader " + (loader == null ? this.getClass()
              .getClassLoader() : loader)
              + " for method " + toString() + " with ID = " + toHexHashString()
      );
      cls = Class
          .forName(className, true, loader == null ? this.getClass().getClassLoader() : loader);
    } catch (Throwable e) {
      RayLog.core.error("Cannot load class {}", className, e);
      return null;
    }

    Method[] ms = cls.getDeclaredMethods();
    ArrayList<Method> methods = new ArrayList<>();
    Type t = Type.getMethodType(this.methodDesc);
    Type[] params = t.getArgumentTypes();
    String rt = t.getReturnType().getDescriptor();

    for (Method m : ms) {
      if (m.getName().equals(methodName)) {
        if (!Arrays.equals(params, Type.getArgumentTypes(m))) {
          continue;
        }

        String mrt = Type.getDescriptor(m.getReturnType());
        if (!rt.equals(mrt)) {
          continue;
        }

        if (isStatic != Modifier.isStatic(m.getModifiers())) {
          continue;
        }

        methods.add(m);
      }
    }

    if (methods.size() != 1) {
      RayLog.core.error(
          "Load method {} failed as there are {} definitions.", toString(), methods.size());
      return null;
    }

    return methods.get(0);
  }

  private byte[] getSha1Hash0() {
    byte[] digests = Sha1Digestor.digest(encoding);
    ByteBuffer bb = ByteBuffer.wrap(digests);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    if (methodName.contains("createActorStage1")) {
      bb.putLong(Long.BYTES, 1);
    } else {
      bb.putLong(Long.BYTES, 0);
    }
    return digests;
  }

  public byte[] getSha1Hash() {
    return digest;
  }

  private String toHexHashString() {
    byte[] id = this.getSha1Hash();
    return StringUtil.toHexHashString(id);
  }

  public String toEncodingString() {
    return encoding;
  }


  @Override
  public int hashCode() {
    return encoding.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MethodId other = (MethodId) obj;
    return className.equals(other.className)
        && methodName.equals(other.methodName)
        && methodDesc.equals(other.methodDesc)
        && isStatic == other.isStatic;
  }

  @Override
  public String toString() {
    return encoding;
  }

}