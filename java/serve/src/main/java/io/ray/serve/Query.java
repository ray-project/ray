package io.ray.serve;

/** Wrap request arguments and meta data. */
public class Query {

  private Object[] args;

  private RequestMetadata metadata;

  public Query(Object[] args, RequestMetadata requestMetadata) {
    this.args = args;
    this.metadata = requestMetadata;
  }

  public Object[] getArgs() {
    return args;
  }

  public void setArgs(Object[] args) {
    this.args = args;
  }

  public RequestMetadata getMetadata() {
    return metadata;
  }

  public void setMetadata(RequestMetadata metadata) {
    this.metadata = metadata;
  }
}
