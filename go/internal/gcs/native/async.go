// Copyright 2026 The Ray Authors.
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

package native

import (
	"context"
)

// asyncResult wraps the result of an asynchronous operation.
type asyncResult[T any] struct {
	data T
	err  error
}

// runAsync executes a blocking CGO call in a dedicated goroutine.
func runAsync[T any](ctx context.Context, fn func() (T, error)) <-chan asyncResult[T] {
	ch := make(chan asyncResult[T], 1)
	go func() {
		// Check cancellation before starting the blocking call.
		select {
		case <-ctx.Done():
			ch <- asyncResult[T]{err: ctx.Err()}
			return
		default:
		}

		data, err := fn()

		// Re-check cancellation before delivering the result.
		select {
		case <-ctx.Done():
			// Cancelled, do not send the result.
			return
		case ch <- asyncResult[T]{data, err}:
		}
	}()
	return ch
}

// waitContext blocks until the asynchronous result is available or the context
// is cancelled.
func waitContext[T any](ctx context.Context, ch <-chan asyncResult[T]) (T, error) {
	var zero T
	select {
	case result := <-ch:
		return result.data, result.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}
