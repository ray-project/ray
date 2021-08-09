package io.ray.serve;

import java.io.Serializable;
import java.util.Map;

/** The meta data of request. */
public class RequestMetadata implements Serializable {

  private static final long serialVersionUID = -8925036926565326811L;

  private String requestId;

  private String endpoint;

  private String callMethod = "call";

  private String httpMethod;

  private Map<String, String> httpHeaders;

  public String getRequestId() {
    return requestId;
  }

  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getCallMethod() {
    return callMethod;
  }

  public void setCallMethod(String callMethod) {
    this.callMethod = callMethod;
  }

  public String getHttpMethod() {
    return httpMethod;
  }

  public void setHttpMethod(String httpMethod) {
    this.httpMethod = httpMethod;
  }

  public Map<String, String> getHttpHeaders() {
    return httpHeaders;
  }

  public void setHttpHeaders(Map<String, String> httpHeaders) {
    this.httpHeaders = httpHeaders;
  }
}
