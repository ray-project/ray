import ray

ray.init("auto")
# Workers and head are annotated as having 5 Custom2 each, so this should trigger upscaling
# of two workers. (One of the bundles will be "placed" on the head.)
ray.autoscaler.sdk.request_resources(bundles=[{"Custom2": 3}, {"Custom2": 3}, {"Custom2": 3}])
