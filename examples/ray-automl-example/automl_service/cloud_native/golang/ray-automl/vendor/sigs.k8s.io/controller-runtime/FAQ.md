# FAQ

### Q: How do I know which type of object a controller references?

**A**: Each controller should only reconcile one object type.  Other
affected objects should be mapped to a single type of root object, using
the `EnqueueRequestForOwner` or `EnqueueRequestsFromMapFunc` event
handlers, and potentially indices. Then, your Reconcile method should
attempt to reconcile *all* state for that given root objects.

### Q: How do I have different logic in my reconciler for different types of events (e.g. create, update, delete)?

**A**: You should not.  Reconcile functions should be idempotent, and
should always reconcile state by reading all the state it needs, then
writing updates.  This allows your reconciler to correctly respond to
generic events, adjust to skipped or coalesced events, and easily deal
with application startup.  The controller will enqueue reconcile requests
for both old and new objects if a mapping changes, but it's your
responsibility to make sure you have enough information to be able clean
up state that's no longer referenced.

### Q: My cache might be stale if I read from a cache! How should I deal with that?

**A**: There are several different approaches that can be taken, depending
on your situation.

- When you can, take advantage of optimistic locking: use deterministic
  names for objects you create, so that the Kubernetes API server will
  warn you if the object already exists.  Many controllers in Kubernetes
  take this approach: the StatefulSet controller appends a specific number
  to each pod that it creates, while the Deployment controller hashes the
  pod template spec and appends that.

- In the few cases when you cannot take advantage of deterministic names
  (e.g. when using generateName), it may be useful in to track which
  actions you took, and assume that they need to be repeated if they don't
  occur after a given time (e.g. using a requeue result).  This is what
  the ReplicaSet controller does.

In general, write your controller with the assumption that information
will eventually be correct, but may be slightly out of date. Make sure
that your reconcile function enforces the entire state of the world each
time it runs.  If none of this works for you, you can always construct
a client that reads directly from the API server, but this is generally
considered to be a last resort, and the two approaches above should
generally cover most circumstances.

### Q: Where's the fake client?  How do I use it?

**A**: The fake client
[exists](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/client/fake),
but we generally recommend using
[envtest.Environment](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/envtest#Environment)
to test against a real API server.  In our experience, tests using fake
clients gradually re-implement poorly-written impressions of a real API
server, which leads to hard-to-maintain, complex test code.

### Q: How should I write tests?  Any suggestions for getting started?

- Use the aforementioned
  [envtest.Environment](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/envtest#Environment)
  to spin up a real API server instead of trying to mock one out.

- Structure your tests to check that the state of the world is as you
  expect it, *not* that a particular set of API calls were made, when
  working with Kubernetes APIs.  This will allow you to more easily
  refactor and improve the internals of your controllers without changing
  your tests.

- Remember that any time you're interacting with the API server, changes
  may have some delay between write time and reconcile time.

### Q: What are these errors about no Kind being registered for a type?

**A**: You're probably missing a fully-set-up Scheme.  Schemes record the
mapping between Go types and group-version-kinds in Kubernetes. In
general, your application should have its own Scheme containing the types
from the API groups that it needs (be they Kubernetes types or your own).
See the [scheme builder
docs](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/scheme) for
more information.
