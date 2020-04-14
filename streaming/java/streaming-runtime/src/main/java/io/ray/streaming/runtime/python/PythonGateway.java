package io.ray.streaming.runtime.python;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonPartition;
import io.ray.streaming.python.stream.PythonStreamSource;
import io.ray.streaming.runtime.util.ReflectionUtils;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.msgpack.core.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gateway for streaming python api.
 * All calls on DataStream in python will be mapped to DataStream call in java by this
 * PythonGateway using ray calls.
 * <p>
 * Note: this class needs to be in sync with `GatewayClient` in
 * `streaming/python/runtime/gateway_client.py`
 */
@SuppressWarnings("unchecked")
public class PythonGateway {
  private static final Logger LOG = LoggerFactory.getLogger(PythonGateway.class);
  private static final String REFERENCE_ID_PREFIX = "__gateway_reference_id__";

  private MsgPackSerializer serializer;
  private Map<String, Object> referenceMap;
  private StreamingContext streamingContext;

  public PythonGateway() {
    serializer = new MsgPackSerializer();
    referenceMap = new HashMap<>();
    LOG.info("PythonGateway created");
  }

  public byte[] createStreamingContext() {
    streamingContext = StreamingContext.buildContext();
    LOG.info("StreamingContext created");
    referenceMap.put(getReferenceId(streamingContext), streamingContext);
    return serializer.serialize(getReferenceId(streamingContext));
  }

  public StreamingContext getStreamingContext() {
    return streamingContext;
  }

  public byte[] withConfig(byte[] confBytes) {
    Preconditions.checkNotNull(streamingContext);
    try {
      Map<String, String> config = (Map<String, String>) serializer.deserialize(confBytes);
      LOG.info("Set config {}", config);
      streamingContext.withConfig(config);
      // We can't use `return void`, that will make `ray.get()` hang forever.
      // We can't using `return new byte[0]`, that will make `ray::CoreWorker::ExecuteTask` crash.
      // So we `return new byte[1]` for method execution success.
      // Same for other methods in this class which return new byte[1].
      return new byte[1];
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] createPythonStreamSource(byte[] pySourceFunc) {
    Preconditions.checkNotNull(streamingContext);
    try {
      PythonStreamSource pythonStreamSource = PythonStreamSource.from(
          streamingContext, PythonFunction.fromFunction(pySourceFunc));
      referenceMap.put(getReferenceId(pythonStreamSource), pythonStreamSource);
      return serializer.serialize(getReferenceId(pythonStreamSource));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] execute(byte[] jobNameBytes) {
    LOG.info("Starting executing");
    streamingContext.execute((String) serializer.deserialize(jobNameBytes));
    // see `withConfig` method.
    return new byte[1];
  }

  public byte[] createPyFunc(byte[] pyFunc) {
    PythonFunction function = PythonFunction.fromFunction(pyFunc);
    referenceMap.put(getReferenceId(function), function);
    return serializer.serialize(getReferenceId(function));
  }

  public byte[] createPyPartition(byte[] pyPartition) {
    PythonPartition partition = new PythonPartition(pyPartition);
    referenceMap.put(getReferenceId(partition), partition);
    return serializer.serialize(getReferenceId(partition));
  }

  public byte[] callFunction(byte[] paramsBytes) {
    try {
      List<Object> params = (List<Object>) serializer.deserialize(paramsBytes);
      params = processReferenceParameters(params);
      LOG.info("callFunction params {}", params);
      String className = (String) params.get(0);
      String funcName = (String) params.get(1);
      Class<?> clz = Class.forName(className, true, this.getClass().getClassLoader());
      Method method = ReflectionUtils.findMethod(clz, funcName);
      Object result = method.invoke(null, params.subList(2, params.size()).toArray());
      referenceMap.put(getReferenceId(result), result);
      return serializer.serialize(getReferenceId(result));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] callMethod(byte[] paramsBytes) {
    try {
      List<Object> params = (List<Object>) serializer.deserialize(paramsBytes);
      params = processReferenceParameters(params);
      LOG.info("callMethod params {}", params);
      Object obj = params.get(0);
      String methodName = (String) params.get(1);
      Method method = ReflectionUtils.findMethod(obj.getClass(), methodName);
      Object result = method.invoke(obj, params.subList(2, params.size()).toArray());
      referenceMap.put(getReferenceId(result), result);
      return serializer.serialize(getReferenceId(result));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<Object> processReferenceParameters(List<Object> params) {
    return params.stream().map(this::processReferenceParameter)
        .collect(Collectors.toList());
  }

  private Object processReferenceParameter(Object o) {
    if (o instanceof String) {
      Object value = referenceMap.get(o);
      if (value != null) {
        return value;
      }
    }
    return o;
  }

  private String getReferenceId(Object o) {
    return REFERENCE_ID_PREFIX + System.identityHashCode(o);
  }

}
