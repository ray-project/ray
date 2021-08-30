package io.ray.serve;

import io.ray.serve.generated.HTTPRequestWrapper;
import io.ray.serve.generated.RequestMetadata;

/** Wrap request arguments and meta data. */
public class Query {

  private RequestMetadata metadata;

  private HTTPRequestWrapper args;

  public Query(RequestMetadata requestMetadata, HTTPRequestWrapper args) {
    this.metadata = requestMetadata;
    this.args = args;
  }

  public RequestMetadata getMetadata() {
    return metadata;
  }

  public void setMetadata(RequestMetadata metadata) {
    this.metadata = metadata;
  }

  public HTTPRequestWrapper getArgs() {
    return args;
  }

  public void setArgs(HTTPRequestWrapper args) {
    this.args = args;
  }
}
