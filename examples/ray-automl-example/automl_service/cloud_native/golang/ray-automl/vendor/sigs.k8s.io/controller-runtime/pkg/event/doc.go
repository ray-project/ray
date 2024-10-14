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
Package event contains the definitions for the Event types produced by source.Sources and transformed into
reconcile.Requests by handler.EventHandler.

You should rarely need to work with these directly -- instead, use Controller.Watch with
source.Sources and handler.EventHandlers.

Events generally contain both a full runtime.Object that caused the event, as well
as a direct handle to that object's metadata.  This saves a lot of typecasting in
code that works with Events.
*/
package event
