package org.ray.streaming.runtime.python;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.msgpack.core.Preconditions;
import org.ray.api.annotation.RayRemote;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.python.PythonFunction;
import org.ray.streaming.python.PythonPartition;
import org.ray.streaming.python.stream.PythonStreamSource;
import org.ray.streaming.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gateway for streaming python api.
 * All calls on DataStream in python will be mapped to DataStream call in java by this
 * PythonGateway using ray calls.
 * <p>
 * Note: this class needs to be in sync with `ray.streaming.gateway_client.GatewayClient`
 */
@SuppressWarnings("unchecked")
@RayRemote
public class PythonGateway {
  private static final Logger LOGGER = LoggerFactory.getLogger(PythonGateway.class);

  private MsgPackSerializer serializer;
  private Map<Integer, Object> objectMap;
  private StreamingContext streamingContext;

  public PythonGateway() {
    serializer = new MsgPackSerializer();
    objectMap = new HashMap<>();
    LOGGER.info("PythonGateway created");
  }

  public byte[] createStreamingContext() {
    streamingContext = StreamingContext.buildContext();
    LOGGER.info("StreamingContext created");
    objectMap.put(getRefId(streamingContext), streamingContext);
    return serializer.serialize(getRefId(streamingContext));
  }

  public void withConfig(byte[] confBytes) {
    Preconditions.checkNotNull(streamingContext);
    try {
      Map<String, String> config = (Map<String, String>) serializer.deserialize(confBytes);
      LOGGER.info("Set config {}", config);
      streamingContext.withConfig(config);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] createPythonStreamSource(byte[] pySourceFunc) {
    Preconditions.checkNotNull(streamingContext);
    try {
      PythonStreamSource pythonStreamSource = PythonStreamSource.from(
          streamingContext, PythonFunction.fromFunction(pySourceFunc));
      objectMap.put(getRefId(pythonStreamSource), pythonStreamSource);
      return serializer.serialize(getRefId(pythonStreamSource));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void execute(byte[] jobNameBytes) {
    LOGGER.info("Starting executing");
    streamingContext.execute((String) serializer.deserialize(jobNameBytes));
  }

  public byte[] createPyFunc(byte[] pyFunc) {
    PythonFunction function = PythonFunction.fromFunction(pyFunc);
    objectMap.put(getRefId(function), function);
    return serializer.serialize(getRefId(function));
  }

  public byte[] createPyPartition(byte[] pyPartition) {
    PythonPartition partition = new PythonPartition(pyPartition);
    objectMap.put(getRefId(partition), partition);
    return serializer.serialize(getRefId(partition));
  }

  public byte[] callFunction(byte[] paramsBytes) {
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

  public byte[] callMethod(byte[] paramsBytes) {
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

