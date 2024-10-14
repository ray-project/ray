/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Package handler defines EventHandlers that enqueue reconcile.Requests in response to Create, Update, Deletion Events
observed from Watching Kubernetes APIs.  Users should provide a source.Source and handler.EventHandler to
Controller.Watch in order to generate and enqueue reconcile.Request work items.

Generally, following premade event handlers should be sufficient for most use cases:

EventHandlers:

EnqueueRequestForObject - Enqueues a reconcile.Request containing the Name and Namespace of the object in the Event.  This will
cause the object that was the source of the Event (e.g. the created / deleted / updated object) to be
reconciled.

EnqueueRequestForOwner - Enqueues a reconcile.Request containing the Name and Namespace of the Owner of the object in the Event.
This will cause owner of the object that was the source of the Event (e.g. the owner object that created the object)
to be reconciled.

EnqueueRequestsFromMapFunc - Enqueues reconcile.Requests resulting from a user provided transformation function run against the
object in the Event.  This will cause an arbitrary collection of objects (defined from a transformation of the
source object) to be reconciled.
*/
package handler
