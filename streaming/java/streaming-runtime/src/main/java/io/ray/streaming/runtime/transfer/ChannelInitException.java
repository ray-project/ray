package io.ray.streaming.runtime.transfer;

import java.util.ArrayList;
import java.util.List;

public class ChannelInitException extends Exception {

  private final List<byte[]> abnormalQueues;

  public ChannelInitException(String message, List<byte[]> abnormalQueues) {
    super(message);
    this.abnormalQueues = abnormalQueues;
  }

  public List<byte[]> getAbnormalChannels() {
    return abnormalQueues;
  }

  public List<String> getAbnormalChannelsString() {
    List<String> res = new ArrayList<>();
    abnormalQueues.forEach(ele -> res.add(ChannelID.idBytesToStr(ele)));
    return res;
  }
}
