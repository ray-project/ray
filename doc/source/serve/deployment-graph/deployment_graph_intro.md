# Intro to Deployment Graphs

```{note}
Note: This feature is in Alpha, so APIs are subject to change.
```

This section should help you:

* compose your Ray Serve deployments together with the **Deployment Graph** API
* serve your applications that use multi-model inference, ensemble models, ML model composition, or mixed business logic/model inference workloads
* independently scale each of your ML models and business logic steps

Ray Serve's **Deployment Graph** API lets you compose your deployments together by describing how to route a request through your deployments. This is particularly useful if you're using ML model composition or mixing business logic and model inference in your application. You can encapsulate each of your models and each of your business logic steps in independent deployments. Then, you can chain these deployments together in a deployment graph.



