(observability)=

# Monitoring and Debugging

This section covers how to **monitor and debug Ray applications and clusters** with Ray's Observability features.


## What is Observability
In general, Observability is a measure of how well internal states of a system can be inferred from knowledge of its external outputs.

In Ray's context, Observability refers to the ability for users to observe and infer Ray applications' and Ray clusters' internal states via various external outputs, such as logs, metrics, events, etc.

![what is ray's observability](./images/what-is-ray-observability.png)


## Why is Observability important for Ray
Debugging a distributed system can be very hard due to its high scalability and complexities. Good Observability is important for Ray users to be able to easily monitor and debug their Ray applications and clusters.

![Importance of observability](./images/importance-of-observability.png)


## Monitoring and debuggging workflow and tools

Toubleshooting is an iterative process and it includes 4 main steps:
1. Monitor the clusters and applications
2. Identify the surfaced problems or errors
3. Debug with various tools and data
4. Form a hypothesis, implement a fix, and validate it.

You'll Learn about all the available Observability tools Ray provides to accerelate your troubleshooting workflow in the rest of this section.

