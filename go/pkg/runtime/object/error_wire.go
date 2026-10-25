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

package object

import (
	"bytes"
	"fmt"
	"strconv"

	protogo "github.com/ray-project/ray/go/proto"
	"github.com/vmihailenco/msgpack/v5"
	goproto "google.golang.org/protobuf/proto"
)

// messagePackOffset is the size of the cross-language length header: a
// msgpack-encoded int holding the payload-section length, zero-padded to 9
// bytes. Byte-format identical to encodeLengthHeader in
// go/internal/runtime/serializer/msgpack_serializer.go; kept local because
// that helper is unexported and the serializer is an internal package. The
// cross-package drift guard lives in that package's test
// (TestErrorLengthHeaderMatchesCanonical in
// go/internal/runtime/serializer/msgpack_serializer_test.go) and compares this
// header against the canonical helper directly.
const messagePackOffset = 9

// TaskExecutionErrorMetadata returns the metadata marking a task execution
// error object under the cross-language numeric convention: the decimal
// rpc::ErrorType number (RayObject::IsException convention). Python's
// _deserialize_object parses error metadata as this integer
// (python/ray/_private/serialization.py) and dispatches
// TASK_EXECUTION_EXCEPTION payloads to RayError.from_bytes.
func TaskExecutionErrorMetadata() []byte {
	return []byte(strconv.Itoa(RpcErrorTypeTaskExecutionException))
}

// MarshalTaskExecutionErrorData builds the data payload for a task execution
// error object: a RayException protobuf (Language GO, formatted message),
// serialized, msgpack-encoded as a single bin, then prefixed with the 9-byte
// length header that split_buffer
// (python/ray/includes/serialization.pxi, kMessagePackOffset = 9) expects.
// Python's TASK_EXECUTION_EXCEPTION branch then reconstructs a
// CrossLanguageError carrying formattedExceptionString, so a Go task's real
// error becomes readable to any Ray language.
func MarshalTaskExecutionErrorData(formattedMessage string) ([]byte, error) {
	pbBytes, err := goproto.Marshal(&protogo.RayException{
		Language:                 protogo.Language_GO,
		FormattedExceptionString: formattedMessage,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal RayException: %w", err)
	}
	section, err := msgpack.Marshal(pbBytes)
	if err != nil {
		return nil, fmt.Errorf("msgpack-encode RayException payload: %w", err)
	}
	var headerBuf bytes.Buffer
	enc := msgpack.NewEncoder(&headerBuf)
	enc.UseCompactInts(true)
	if err := enc.EncodeInt(int64(len(section))); err != nil {
		return nil, fmt.Errorf("encode error length header: %w", err)
	}
	if headerBuf.Len() > messagePackOffset {
		return nil, fmt.Errorf("error length header exceeds %d bytes", messagePackOffset)
	}
	header := make([]byte, messagePackOffset)
	copy(header, headerBuf.Bytes())
	return append(header, section...), nil
}

// taskExceptionFromGoWirePayload parses the Go writer's error payload (see
// MarshalTaskExecutionErrorData). It returns nil when the payload does not
// match this dialect, so the caller falls back to the C++ RayErrorInfo
// dialect handled by extractErrorInfoMessage.
func taskExceptionFromGoWirePayload(data []byte) RayException {
	if len(data) <= messagePackOffset {
		return nil
	}
	dec := msgpack.NewDecoder(bytes.NewReader(data[:messagePackOffset]))
	length, err := dec.DecodeInt64()
	if err != nil || length <= 0 || length > int64(len(data)-messagePackOffset) {
		return nil
	}
	var pbBytes []byte
	if err := msgpack.Unmarshal(data[messagePackOffset:messagePackOffset+int(length)], &pbBytes); err != nil {
		return nil
	}
	var rayExc protogo.RayException
	if err := goproto.Unmarshal(pbBytes, &rayExc); err != nil {
		return nil
	}
	// Only a Go RayException belongs to this dialect. RayErrorInfo has no field
	// 1, so a C++ error payload mis-parsed as a RayException always yields
	// language 0; rejecting it here falls through to extractErrorInfoMessage
	// instead of returning its field-3 sub-message bytes as a message.
	if rayExc.GetLanguage() != protogo.Language_GO {
		return nil
	}
	message := rayExc.GetFormattedExceptionString()
	if message == "" {
		return nil
	}
	return NewRayTaskExecutionException("", fmt.Errorf("%s", message), "")
}
