package org.ray.streaming.python;


import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.msgpack.core.Preconditions;
import org.ray.api.annotation.RayRemote;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.Stream;
import org.ray.streaming.python.descriptor.DescriptorFunction;
import org.ray.streaming.python.descriptor.DescriptorPartition;
import org.ray.streaming.python.stream.PythonStreamSource;
import org.ray.streaming.util.ReflectionUtils;

/**
 * Gateway for streaming python api.
 * All calls on DataStream in python will be mapped to DataStream call in java by this
 * PythonGateway using ray calls.
 * <p>
 * FIXME Do we need use py4j as GatewayServer
 */
@SuppressWarnings("unchecked")
@RayRemote
public class PythonGateway {
  private MsgPackSerializer serializer;
  private Map<Integer, Object> objectMap;
  private StreamingContext streamingContext;

  public PythonGateway() {
    serializer = new MsgPackSerializer();
    objectMap = new HashMap<>();
  }

  public byte[] createStreamingContext() {
    streamingContext = StreamingContext.buildContext();
    objectMap.put(getRefId(streamingContext), streamingContext);
    return serializer.serialize(getRefId(streamingContext));
  }

  public void withConfig(byte[] confBytes) {
    Preconditions.checkNotNull(streamingContext);
    try {
      Map<String, String> config = (Map<String, String>) serializer.deserialize(confBytes);
      streamingContext.withConfig(config);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void execute() {
    streamingContext.execute();
  }

  public byte[] createPyFunc(byte[] pyFunc) {
    DescriptorFunction function = new DescriptorFunction(pyFunc);
    objectMap.put(getRefId(function), function);
    return serializer.serialize(getRefId(function));
  }

  public byte[] createPyPartition(byte[] pyPartition) {
    DescriptorPartition partition = new DescriptorPartition(pyPartition);
    objectMap.put(getRefId(partition), partition);
    return serializer.serialize(getRefId(partition));
  }

  public byte[] createPythonStreamSource(byte[] pySourceFunc) {
    Preconditions.checkNotNull(streamingContext);
    try {
      PythonStreamSource pythonStreamSource = new PythonStreamSource(
          streamingContext, new DescriptorFunction(pySourceFunc));
      objectMap.put(getRefId(pythonStreamSource), pythonStreamSource);
      return serializer.serialize(getRefId(pythonStreamSource));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] funcCall(byte[] paramsBytes) {
    try {
      List<Object> params = (List<Object>) serializer.deserialize(paramsBytes);
      String className = (String) params.get(0);
      String funcName = (String) params.get(1);
      Class<?> clz = Class.forName(className, true, this.getClass().getClassLoader());
      Method method = ReflectionUtils.findMethod(clz, funcName);
      Object result = method.invoke(null, params.subList(2, params.size()).toArray());
      objectMap.put(getRefId(result), result);
      return serializer.serialize(getRefId(result));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] methodCall(byte[] paramsBytes) {
    try {
      List<Object> params = (List<Object>) serializer.deserialize(paramsBytes);
      Integer objId = (Integer) params.get(0);
      Object obj = objectMap.get(objId);
      String methodName = (String) params.get(1);
      Method method = ReflectionUtils.findMethod(obj.getClass(), methodName);
      Object result = method.invoke(obj, params.subList(2, params.size()).toArray());
      objectMap.put(getRefId(result), result);
      return serializer.serialize(getRefId(result));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int getRefId(Object o) {
    return System.identityHashCode(o);
  }

}

