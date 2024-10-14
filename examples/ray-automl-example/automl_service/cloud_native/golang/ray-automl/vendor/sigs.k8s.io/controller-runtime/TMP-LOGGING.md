Logging Guidelines
==================

controller-runtime uses a kind of logging called *structured logging*. If
you've used a library like Zap or logrus before, you'll be familiar with
the concepts we use.  If you've only used a logging library like the "log"
package (in the Go standard library) or "glog" (in Kubernetes), you'll
need to adjust how you think about logging a bit.

### Getting Started With Structured Logging

With structured logging, we associate a *constant* log message with some
variable key-value pairs.  For instance, suppose we wanted to log that we
were starting reconciliation on a pod.  In the Go standard library logger,
we might write:

```go
log.Printf("starting reconciliation for pod %s/%s", podNamespace, podName)
```

In controller-runtime, we'd instead write:

```go
logger.Info("starting reconciliation", "pod", req.NamespacedNamed)
```

or even write

```go
func (r *Reconciler) Reconcile(req reconcile.Request) (reconcile.Response, error) {
    logger := logger.WithValues("pod", req.NamespacedName)
    // do some stuff
    logger.Info("starting reconciliation")
}
```

Notice how we've broken out the information that we want to convey into
a constant message (`"starting reconciliation"`) and some key-value pairs
that convey variable information (`"pod", req.NamespacedName`).  We've
there-by added "structure" to our logs, which makes them easier to save
and search later, as well as correlate with metrics and events.

All of controller-runtime's logging is done via
[logr](https://github.com/go-logr/logr), a generic interface for
structured logging.  You can use whichever logging library you want to
implement the actual mechanics of the logging.  controller-runtime
provides some helpers to make it easy to use
[Zap](https://go.uber.org/zap) as the implementation.

You can configure the logging implementation using
`"sigs.k8s.io/controller-runtime/pkg/log".SetLogger`.  That
package also contains the convenience functions for setting up Zap.

You can get a handle to the the "root" logger using
`"sigs.k8s.io/controller-runtime/pkg/log".Log`, and can then call
`WithName` to create individual named loggers.  You can call `WithName`
repeatedly to chain names together:

```go
logger := log.Log.WithName("controller").WithName("replicaset")
// in reconcile...
logger = logger.WithValues("replicaset", req.NamespacedName)
// later on in reconcile...
logger.Info("doing things with pods", "pod", newPod)
```

As seen above, you can also call `WithValue` to create a new sub-logger
that always attaches some key-value pairs to a logger.

Finally, you can use `V(1)` to mark a particular log line as "debug" logs:

```go
logger.V(1).Info("this is particularly verbose!", "state of the world",
allKubernetesObjectsEverywhere)
```

While it's possible to use higher log levels, it's recommended that you
stick with `V(1)` or `V(0)` (which is equivalent to not specifying `V`),
and then filter later based on key-value pairs or messages; different
numbers tend to lose meaning easily over time, and you'll be left
wondering why particular logs lines are at `V(5)` instead of `V(7)`.

## Logging errors

Errors should *always* be logged with `log.Error`, which allows logr
implementations to provide special handling of errors (for instance,
providing stack traces in debug mode).

It's acceptable to log call `log.Error` with a nil error object.  This
conveys that an error occurred in some capacity, but that no actual
`error` object was involved.

Errors returned by the `Reconcile` implementation of the `Reconciler` interface are commonly logged as a `Reconciler error`.
It's a developer choice to create an additional error log in the `Reconcile` implementation so a more specific file name and line for the error are returned. 

## Logging messages

- Don't put variable content in your messages -- use key-value pairs for
  that. Never use `fmt.Sprintf` in your message.

- Try to match the terminology in your messages with your key-value pairs
  -- for instance, if you have a key-value pairs `api version`, use the
  term `APIVersion` instead of `GroupVersion` in your message.

## Logging Kubernetes Objects

Kubernetes objects should be logged directly, like `log.Info("this is
a Kubernetes object", "pod", somePod)`.  controller-runtime provides
a special encoder for Zap that will transform Kubernetes objects into
`name, namespace, apiVersion, kind` objects, when available and not in
development mode.  Other logr implementations should implement similar
logic.

## Logging Structured Values (Key-Value pairs)

- Use lower-case, space separated keys.  For example `object` for objects,
  `api version` for `APIVersion`

- Be consistent across your application, and with controller-runtime when
  possible.

- Try to be brief but descriptive.

- Match terminology in keys with terminology in the message.

- Be careful logging non-Kubernetes objects verbatim if they're very
  large.

### Groups, Versions, and Kinds

- Kinds should not be logged alone (they're meaningless alone).  Use
  a `GroupKind` object to log them instead, or a `GroupVersionKind` when
  version is relevant.

- If you need to log an API version string, use `api version` as the key
  (formatted as with a `GroupVersion`, or as received directly from API
  discovery).

### Objects and Types

- If code works with a generic Kubernetes `runtime.Object`, use the
  `object` key.  For specific objects, prefer the resource name as the key
  (e.g. `pod` for `v1.Pod` objects).

- For non-Kubernetes objects, the `object` key may also be used, if you
  accept a generic interface.

- When logging a raw type, log it using the `type` key, with a value of
  `fmt.Sprintf("%T", typ)`

- If there's specific context around a type, the key may be more specific,
  but should end with `type` -- for instance, `OwnerType` should be logged
  as `owner` in the context of `log.Error(err, "Could not get ObjectKinds
  for OwnerType", `owner type`, fmt.Sprintf("%T"))`.  When possible, favor
  communicating kind instead.

### Multiple things

- When logging multiple things, simply pluralize the key.

### controller-runtime Specifics

- Reconcile requests should be logged as `request`, although normal code
  should favor logging the key.

- Reconcile keys should be logged as with the same key as if you were
  logging the object directly (e.g. `log.Info("reconciling pod", "pod",
  req.NamespacedName)`).  This ends up having a similar effect to logging
  the object directly.
