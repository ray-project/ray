/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2018 All Rights Reserved.
 */
package org.ray.util;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import jdk.internal.org.objectweb.asm.Type;
import org.ray.util.logger.RayLog;

/**
 *
 */
public final class MethodId {

  /**
   * SerializedLambda.implMethodKind
   * REF_NONE                    = 0
   * REF_getField                = 1,
   * REF_getStatic               = 2,
   * REF_putField                = 3,
   * REF_putStatic               = 4,
   * REF_invokeVirtual           = 5,
   * REF_invokeStatic            = 6,
   * REF_invokeSpecial           = 7,
   * REF_newInvokeSpecial        = 8,
   * REF_invokeInterface         = 9,
   * REF_LIMIT                  = 10;
   */

  private static final ConcurrentHashMap<String, MethodId> MAP = new ConcurrentHashMap<>(256);
  private final String className;
  private final String methodName;
  private final String methodDesc;
  private final boolean isStatic;
  private final String encoding;

  private final byte[] digest;

  private MethodId(String className, String methodName, String methodDesc, boolean isStatic,
      String encoding) {
    this.className = className;
    this.methodName = methodName;
    this.methodDesc = methodDesc;
    this.isStatic = isStatic;
    this.encoding = encoding;
    this.digest = getSha1Hash0();
  }

  public MethodId(String className, String methodName, String methodDesc, boolean isStatic) {
    this(className, methodName, methodDesc, isStatic,
        encoding(className, methodName, methodDesc, isStatic));
  }

  private static String encoding(String className, String methodName, String methodDesc,
      boolean isStatic) {
    StringBuilder sb = new StringBuilder(512);
    sb.append(className).append('.').append(methodName).append("::").append(methodDesc).append("&&")
        .append(isStatic);
    return sb.toString();
  }

  public static MethodId transferSerializedLambda(SerializedLambda lambda) {
    return transferSerializedLambda(lambda, false);
  }

  public static MethodId transferSerializedLambda(SerializedLambda lambda, boolean forceNew) {
    if (lambda.getCapturedArgCount() != 0) {
      throw new IllegalArgumentException("could not transfer a lambda which is closer");
    }
    //REF_invokeStatic
    boolean isstatic = lambda.getImplMethodKind() == 6;
    final String encoding = encoding(lambda.getImplClass(), lambda.getImplMethodName(),
        lambda.getImplMethodSignature(), isstatic);
    if(forceNew){
      return new MethodId(lambda.getImplClass(), lambda.getImplMethodName(),
          lambda.getImplMethodSignature(), isstatic, encoding);
    }
    MethodId m = MAP.get(encoding);
    if (m == null) {
      m = new MethodId(lambda.getImplClass(), lambda.getImplMethodName(),
          lambda.getImplMethodSignature(), isstatic, encoding);
      MAP.putIfAbsent(encoding, m);
      m = MAP.get(encoding);
    }
    return m;
  }

  public Method load() {
    return load(null);
  }

  public Method load(ClassLoader loader) {
    String loadClsName = className.replace('/', '.');
    Class<?> cls;
    try {
      RayLog.core.debug(
          "load class " + loadClsName + " from class loader " + (loader == null ? this.getClass()
              .getClassLoader() : loader)
              + " for method " + toString() + " with ID = " + toHexHashString()
      );
      cls = Class
          .forName(loadClsName, true, loader == null ? this.getClass().getClassLoader() : loader);
    } catch (Throwable e) {
      RayLog.core.error("Cannot load class " + loadClsName, e);
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
          "Load method " + toString() + " failed as there are " + methods.size() + " definitions");
      return null;
    }

    return methods.get(0);
  }

  private byte[] getSha1Hash0() {
    byte[] digests = Sha1Digestor.getSha1Hash(encoding);
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

  public String getClassName() {
    return className;
  }

  public String getMethodName() {
    return methodName;
  }

  public String getMethodDesc() {
    return methodDesc;
  }

  public boolean isStatic() {
    return isStatic;
  }

}