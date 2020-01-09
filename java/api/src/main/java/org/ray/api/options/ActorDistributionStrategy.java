package org.ray.api.options;

public enum ActorDistributionStrategy {
  RANDOM,  // Default. Create Actors in random places.
  CENTRALIZED,  // Depth-first. Try to create Actors in the same node.
  DECENTRALIZED, // Breadth-first. Try to create Actors in different nodes.
  LABEL,  // User-defined. Assign a label to each Actor, and try to place Actors with the same label in the same node.
}
