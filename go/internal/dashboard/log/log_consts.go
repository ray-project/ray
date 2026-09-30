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

package log

// mimeTypes mirrors MIME_TYPES in log_consts.py: extensions registered as
// text/plain for the log file browser.
var mimeTypes = map[string][]string{
	"text/plain": {".err", ".out", ".log"},
}

// logGRPCError is the trailing-metadata key used by StreamLog to surface a
// non-standard gRPC error to the client. It mirrors LOG_GRPC_ERROR in
// log_consts.py; the client (state_manager.py) awaits the initial metadata and
// raises when this key is present.
const logGRPCError = "log_grpc_status"

// grpcTimeout is the log gRPC call timeout in seconds, mirroring
// GRPC_TIMEOUT in log_consts.py.
const grpcTimeout = 10
