package org.ray.lib.actorgroup.api.options;

public enum LoadBalancingStrategy {
  ROUND_ROBIN,  // Select the next Actor to invoke the remote function
  SHARDING,  // Select the Actor by a user-defined sharding key
  FIRST_REPLY,  // Broadcast the remote function and use the first put result.
}
