// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package gcs provides the Go client for Ray Global Control Store (GCS).
//
// The GCS client enables Go applications to access Ray cluster metadata,
// including nodes, jobs, actors, workers, and placement groups.
//
// Basic usage:
//
//	client, err := gcs.ConnectClient(gcs.ClientOptions{
//	    Address:   "localhost:6379",
//	    TimeoutMs: 10000,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Access cluster state
//	clusterID := client.ClusterID()
//	nodes, err := client.Nodes().GetAll(context.Background(), nil)
package gcs
