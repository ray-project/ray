package io.ray.serve.router;

import io.ray.serve.generated.RequestMetadata;

/** Wrap request arguments and meta data. */
public class Query {

  private RequestMetadata metadata;

  /**
   * If this query is cross-language, the args is serialized {@link
   * io.ray.serve.generated.RequestWrapper}. Otherwise, it is Object[].
   */
  private Object args;

  public Query(RequestMetadata requestMetadata, Object args) {
    this.metadata = requestMetadata;
    this.args = args;
  }

  public RequestMetadata getMetadata() {
    return metadata;
  }

  public Object getArgs() {
    return args;
  }
}
