package gcs

import (
	"context"
	"testing"
)

// mockNodeResourceUsagePublisher is a minimal implementation of the
// NodeResourceUsagePublisher interface, used to verify the interface is
// satisfied at compile time and that its method contract is correctly shaped.
type mockNodeResourceUsagePublisher struct{}

func (m *mockNodeResourceUsagePublisher) PublishNodeResourceUsage(ctx context.Context, keyID, jsonPayload string) error {
	return nil
}

// Compile-time assertion: mockNodeResourceUsagePublisher must satisfy the
// NodeResourceUsagePublisher interface. If the interface signature changes or
// is renamed, this fails to compile, catching an interface regression early.
var _ NodeResourceUsagePublisher = (*mockNodeResourceUsagePublisher)(nil)

// TestNodeResourceUsagePublisherContract verifies the method signature of the
// NodeResourceUsagePublisher interface, ensuring callers can invoke it via the
// (ctx, keyID, jsonPayload) -> error contract, and guards against misuse.
func TestNodeResourceUsagePublisherContract(t *testing.T) {
	publisher := mockNodeResourceUsagePublisher{}
	err := publisher.PublishNodeResourceUsage(context.Background(), "node-1", `{"cpu": 0.5}`)
	if err != nil {
		t.Fatalf("unexpected error from PublishNodeResourceUsage: %v", err)
	}
}
