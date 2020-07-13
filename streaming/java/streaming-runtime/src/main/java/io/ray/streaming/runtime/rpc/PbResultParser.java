package io.ray.streaming.runtime.rpc;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.message.CallResult;
import io.ray.streaming.runtime.transfer.QueueRecoverInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PbResultParser {

  private static final Logger LOG = LoggerFactory.getLogger(PbResultParser.class);

  public static Boolean parseBoolResult(byte[] result) {
    if (null == result) {
      LOG.warn("Result is null.");
      return false;
    }

    RemoteCall.BoolResult boolResult;
    try {
      boolResult = RemoteCall.BoolResult.parseFrom(result);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Parse boolean result has exception.", e);
      return false;
    }

    return boolResult.getBoolRes();
  }

  public static CallResult<QueueRecoverInfo> parseRollbackResult(byte[] bytes) {
    RemoteCall.CallResult callResultPb;
    try {
      callResultPb = RemoteCall.CallResult.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Rollback parse result has exception.", e);
      return CallResult.fail();
    }

    CallResult<QueueRecoverInfo> callResult = new CallResult<>();
    callResult.setSuccess(callResultPb.getSuccess());
    callResult.setResultCode(callResultPb.getResultCode());
    callResult.setResultMsg(callResultPb.getResultMsg());
    RemoteCall.QueueRecoverInfo qRecoverInfo = callResultPb.getResultObj();
    Map<String, QueueRecoverInfo.QueueCreationStatus> creationStatusMap = new HashMap<>();
    qRecoverInfo.getCreationStatusMap().forEach((k, v) -> {
      creationStatusMap.put(k, QueueRecoverInfo.QueueCreationStatus.fromInt(v.getNumber()));
    });
    callResult.setResultObj(new QueueRecoverInfo(creationStatusMap));
    return callResult;
  }
}
