package io.ray.streaming.runtime.rpc;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.message.CallResult;
import io.ray.streaming.runtime.transfer.channel.ChannelRecoverInfo;
import java.util.HashMap;
import java.util.Map;
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

  public static CallResult<ChannelRecoverInfo> parseRollbackResult(byte[] bytes) {
    RemoteCall.CallResult callResultPb;
    try {
      callResultPb = RemoteCall.CallResult.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Rollback parse result has exception.", e);
      return CallResult.fail();
    }

    CallResult<ChannelRecoverInfo> callResult = new CallResult<>();
    callResult.setSuccess(callResultPb.getSuccess());
    callResult.setResultCode(callResultPb.getResultCode());
    callResult.setResultMsg(callResultPb.getResultMsg());
    RemoteCall.QueueRecoverInfo recoverInfo = callResultPb.getResultObj();
    Map<String, ChannelRecoverInfo.ChannelCreationStatus> creationStatusMap = new HashMap<>();
    recoverInfo
        .getCreationStatusMap()
        .forEach(
            (k, v) -> {
              creationStatusMap.put(
                  k, ChannelRecoverInfo.ChannelCreationStatus.fromInt(v.getNumber()));
            });
    callResult.setResultObj(new ChannelRecoverInfo(creationStatusMap));
    return callResult;
  }
}
