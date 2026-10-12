//go:build apipg

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

// Unit tests for the api placement group facade. Gated behind the "apipg"
// build tag because storeHandleLocked mutates package-global handle state
// (currentHandle) that the default api tests assume is unset (see the
// apilocalinit/apiinternal/apireinit precedents). Includes task_return_ref_test.go
// (via the placement_group_test BUILD target) for the shared
// recordingRuntime/recordingHandle injection helpers.
package api

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
	"github.com/ray-project/ray/go/proto"
)

// pgRecordingSubmitter records the placement group calls made through the
// submitter interface. It embeds submitter.TaskSubmitter (nil) so only the
// placement group methods need to be implemented; any other interface method
// is never reached by these tests.
type pgRecordingSubmitter struct {
	submitter.TaskSubmitter

	pgID ids.PlacementGroupID

	createCalled bool
	createOpts   *submitter.PlacementGroupCreationOptions
	createErr    error

	removeCalled bool
	removeID     ids.PlacementGroupID
	removeErr    error

	waitCalled  bool
	waitID      ids.PlacementGroupID
	waitTimeout time.Duration
	waitErr     error
}

func (s *pgRecordingSubmitter) CreatePlacementGroup(ctx context.Context, opts *submitter.PlacementGroupCreationOptions) (ids.PlacementGroupID, error) {
	s.createCalled = true
	s.createOpts = opts
	return s.pgID, s.createErr
}

func (s *pgRecordingSubmitter) RemovePlacementGroup(ctx context.Context, id ids.PlacementGroupID) error {
	s.removeCalled = true
	s.removeID = id
	return s.removeErr
}

func (s *pgRecordingSubmitter) WaitPlacementGroupReady(ctx context.Context, id ids.PlacementGroupID, timeout time.Duration) error {
	s.waitCalled = true
	s.waitID = id
	s.waitTimeout = timeout
	return s.waitErr
}

// pgMockRuntime embeds recordingRuntime (which implements every
// contract.Runtime method) to supply the injected task submitter.
type pgMockRuntime struct {
	recordingRuntime
	sub submitter.TaskSubmitter
}

func (r pgMockRuntime) GetTaskSubmitter() submitter.TaskSubmitter { return r.sub }

// initPgMockRuntime installs a mock runtime whose GetTaskSubmitter returns the
// given submitter, so getTaskSubmitter() reaches it.
func initPgMockRuntime(t *testing.T, sub submitter.TaskSubmitter) {
	t.Helper()
	storeHandleLocked(recordingHandle{rt: pgMockRuntime{sub: sub}})
	t.Cleanup(clearHandle)
}

// pgMockGCSClient is a minimal GCSClient whose placement group methods return
// canned data.
type pgMockGCSClient struct {
	pgData *proto.PlacementGroupTableData
	pgList []*proto.PlacementGroupTableData
	err    error
}

var _ GCSClient = (*pgMockGCSClient)(nil)

func (m *pgMockGCSClient) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return nil, m.err
}

func (m *pgMockGCSClient) NextJobID(ctx context.Context) (string, error) { return "", m.err }

func (m *pgMockGCSClient) Close() error { return nil }

func (m *pgMockGCSClient) IsClosed() bool { return false }

func (m *pgMockGCSClient) GetPlacementGroupInfo(ctx context.Context, id ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return m.pgData, m.err
}

func (m *pgMockGCSClient) GetPlacementGroupInfoByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	return m.pgData, m.err
}

func (m *pgMockGCSClient) GetAllPlacementGroupInfo(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return m.pgList, m.err
}

func (m *pgMockGCSClient) GetInternalKV(ctx context.Context, ns, key string) ([]byte, error) {
	return nil, m.err
}

func (m *pgMockGCSClient) GetAllNodeInfo(ctx context.Context) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return nil, m.err
}

func (m *pgMockGCSClient) GetAllActorInfo(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return nil, m.err
}

// pgMockFactory is a GCSClientFactory that returns a canned client.
type pgMockFactory struct {
	client GCSClient
}

func (f *pgMockFactory) CreateClient(opts gcs.ClientOptions) (GCSClient, error) {
	return f.client, nil
}

// installMockGCSClient registers a factory returning the given client and
// points readRayAddressFromFile at a fixed address so GetGCSClient resolves.
func installMockGCSClient(t *testing.T, client GCSClient) {
	t.Helper()
	const addr = "127.0.0.1:6380"

	origAddr := readRayAddressFromFile
	t.Cleanup(func() { readRayAddressFromFile = origAddr })
	readRayAddressFromFile = func() string { return addr }

	origFactory := getGCSClientFactory()
	t.Cleanup(func() { RegisterGCSClientFactory(origFactory) })
	RegisterGCSClientFactory(&pgMockFactory{client: client})

	t.Cleanup(func() { gcsClientCache.Delete(addr) })
}

func TestPlacementGroupCreationOptionsValidate(t *testing.T) {
	opts, err := NewPlacementGroupCreationOptionsBuilder().
		WithName("pg-1").
		WithBundles([]map[string]float64{{"CPU": 1}}).
		Build()
	if err != nil {
		t.Fatalf("expected valid options, got %v", err)
	}
	if opts.Name != "pg-1" || len(opts.Bundles) != 1 {
		t.Fatalf("unexpected options: %+v", opts)
	}
	if _, err := NewPlacementGroupCreationOptionsBuilder().Build(); err == nil {
		t.Fatal("expected error for empty options")
	}
}

func TestPlacementGroupCreationOptionsAnonymous(t *testing.T) {
	// The name is optional: an anonymous placement group (no name) with a valid
	// bundle list builds and validates successfully, matching the Python and
	// Java APIs.
	opts, err := NewPlacementGroupCreationOptionsBuilder().
		WithBundles([]map[string]float64{{"CPU": 1}}).
		Build()
	if err != nil {
		t.Fatalf("expected anonymous placement group to build, got %v", err)
	}
	if opts.Name != "" || len(opts.Bundles) != 1 {
		t.Fatalf("unexpected options: %+v", opts)
	}
}

func TestCreatePlacementGroupUsesSubmitter(t *testing.T) {
	id := ids.OfPlacementGroupID(ids.NewJobID())
	s := &pgRecordingSubmitter{pgID: id}
	initPgMockRuntime(t, s)

	opts, err := NewPlacementGroupCreationOptionsBuilder().
		WithName("pg-1").
		WithBundles([]map[string]float64{{"CPU": 1}}).
		WithStrategy(PlacementStrategySpread).
		Build()
	if err != nil {
		t.Fatalf("Build error: %v", err)
	}
	pg, err := CreatePlacementGroup(context.Background(), opts)
	if err != nil {
		t.Fatalf("CreatePlacementGroup error: %v", err)
	}
	if pg == nil || pg.ID() != id {
		t.Fatalf("expected %v, got %v", id, pg)
	}
	if pg.Name() != "pg-1" || len(pg.Bundles()) != 1 || pg.Strategy() != PlacementStrategySpread {
		t.Fatalf("pg = %+v", pg)
	}
	if !s.createCalled || s.createOpts == nil {
		t.Fatalf("submitter not called: %+v", s)
	}
	if s.createOpts.Name != "pg-1" {
		t.Fatalf("createOpts.Name = %q, want pg-1", s.createOpts.Name)
	}
	if len(s.createOpts.Bundles) != 1 || s.createOpts.Bundles[0]["CPU"] != 1 {
		t.Fatalf("createOpts.Bundles = %v", s.createOpts.Bundles)
	}
	if s.createOpts.Strategy != int32(PlacementStrategySpread) {
		t.Fatalf("createOpts.Strategy = %d, want %d", s.createOpts.Strategy, int32(PlacementStrategySpread))
	}
}

func TestRemovePlacementGroupUsesSubmitter(t *testing.T) {
	id := ids.OfPlacementGroupID(ids.NewJobID())
	s := &pgRecordingSubmitter{}
	initPgMockRuntime(t, s)

	if err := RemovePlacementGroup(context.Background(), id); err != nil {
		t.Fatalf("RemovePlacementGroup error: %v", err)
	}
	if !s.removeCalled || s.removeID != id {
		t.Fatalf("submitter not called with id %v (called=%v id=%v)", id, s.removeCalled, s.removeID)
	}
}

func TestWaitPlacementGroupReadyUsesSubmitter(t *testing.T) {
	id := ids.OfPlacementGroupID(ids.NewJobID())
	s := &pgRecordingSubmitter{}
	initPgMockRuntime(t, s)

	const timeout = 3 * time.Second
	if err := PlacementGroups.WaitPlacementGroupReady(context.Background(), id, timeout); err != nil {
		t.Fatalf("WaitPlacementGroupReady error: %v", err)
	}
	if !s.waitCalled || s.waitID != id || s.waitTimeout != timeout {
		t.Fatalf("submitter not called correctly: %+v", s)
	}
}

func TestPlacementGroupWait(t *testing.T) {
	id := ids.OfPlacementGroupID(ids.NewJobID())

	t.Run("ready", func(t *testing.T) {
		s := &pgRecordingSubmitter{}
		initPgMockRuntime(t, s)
		pg := &PlacementGroup{id: id}
		ready, err := pg.Wait(3)
		if err != nil {
			t.Fatalf("Wait error: %v", err)
		}
		if !ready {
			t.Fatal("expected ready=true")
		}
		if !s.waitCalled || s.waitID != id || s.waitTimeout != 3*time.Second {
			t.Fatalf("submitter not called correctly: %+v", s)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		s := &pgRecordingSubmitter{waitErr: submitter.ErrPlacementGroupNotReady}
		initPgMockRuntime(t, s)
		pg := &PlacementGroup{id: id}
		ready, err := pg.Wait(3)
		if err != nil {
			t.Fatalf("Wait error: %v", err)
		}
		if ready {
			t.Fatal("expected ready=false on timeout")
		}
	})

	t.Run("real error", func(t *testing.T) {
		realErr := stderrors.New("gcs failure")
		s := &pgRecordingSubmitter{waitErr: realErr}
		initPgMockRuntime(t, s)
		pg := &PlacementGroup{id: id}
		if _, err := pg.Wait(3); err != realErr {
			t.Fatalf("expected %v, got %v", realErr, err)
		}
	})

	t.Run("nil submitter", func(t *testing.T) {
		initPgMockRuntime(t, nil)
		pg := &PlacementGroup{id: id}
		_, err := pg.Wait(3)
		assertRuntimeErrorState(t, err, "wait_placement_group", "submitter_not_available")
	})

	t.Run("non-positive timeout", func(t *testing.T) {
		s := &pgRecordingSubmitter{}
		initPgMockRuntime(t, s)
		pg := &PlacementGroup{id: id}
		if _, err := pg.Wait(0); err == nil {
			t.Fatal("expected error for non-positive timeout")
		}
	})

	t.Run("WaitContext cancelled", func(t *testing.T) {
		// A cancelled context must abort the wait with ctx.Err() even though the
		// submitter is still blocking, mirroring how the facade threads the
		// caller's context into the submitter.
		s := &pgCtxBlockingSubmitter{}
		initPgMockRuntime(t, s)
		pg := &PlacementGroup{id: id}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ready, err := pg.WaitContext(ctx, 60)
		if err == nil {
			t.Fatal("expected ctx cancellation error, got nil")
		}
		if ready {
			t.Fatal("expected ready=false on cancelled context")
		}
	})
}

// pgCtxBlockingSubmitter is a submitter whose WaitPlacementGroupReady blocks
// until the context is cancelled, so tests can verify that the facade threads
// the caller's context through instead of hardcoding a background context.
type pgCtxBlockingSubmitter struct {
	submitter.TaskSubmitter
}

func (s *pgCtxBlockingSubmitter) CreatePlacementGroup(ctx context.Context, opts *submitter.PlacementGroupCreationOptions) (ids.PlacementGroupID, error) {
	return ids.NilPlacementGroupID(), nil
}

func (s *pgCtxBlockingSubmitter) RemovePlacementGroup(ctx context.Context, id ids.PlacementGroupID) error {
	return nil
}

func (s *pgCtxBlockingSubmitter) WaitPlacementGroupReady(ctx context.Context, id ids.PlacementGroupID, timeout time.Duration) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestGetPlacementGroupUsesGCSClient(t *testing.T) {
	jobID := ids.NewJobID()
	pgID := ids.OfPlacementGroupID(jobID)
	client := &pgMockGCSClient{
		pgData: &proto.PlacementGroupTableData{
			PlacementGroupId: pgID.Binary(),
			Name:             "pg-1",
			Strategy:         proto.PlacementStrategy_PACK,
			State:            proto.PlacementGroupTableData_CREATED,
			CreatorJobId:     jobID.Binary(),
			Bundles: []*proto.Bundle{
				{UnitResources: map[string]float64{"CPU": 1.0}},
			},
		},
	}
	installMockGCSClient(t, client)

	pg, err := GetPlacementGroup(context.Background(), pgID)
	if err != nil {
		t.Fatalf("GetPlacementGroup error: %v", err)
	}
	if pg == nil {
		t.Fatal("pg is nil")
	}
	if pg.ID() != pgID {
		t.Fatalf("pg.ID() = %v, want %v", pg.ID(), pgID)
	}
	if pg.Name() != "pg-1" || pg.State() != PlacementGroupStateCreated || pg.Strategy() != PlacementStrategyPack {
		t.Fatalf("pg = %+v", pg)
	}
	if len(pg.Bundles()) != 1 || pg.Bundles()[0]["CPU"] != 1.0 {
		t.Fatalf("pg.Bundles() = %v, want [{CPU:1.0}]", pg.Bundles())
	}
}

func TestGetPlacementGroupByNameUsesGCSClient(t *testing.T) {
	jobID := ids.NewJobID()
	pgID := ids.OfPlacementGroupID(jobID)
	client := &pgMockGCSClient{
		pgData: &proto.PlacementGroupTableData{
			PlacementGroupId: pgID.Binary(),
			Name:             "pg-1",
			Strategy:         proto.PlacementStrategy_SPREAD,
			State:            proto.PlacementGroupTableData_PREPARED,
		},
	}
	installMockGCSClient(t, client)

	pg, err := GetPlacementGroupByName(context.Background(), "pg-1", "")
	if err != nil {
		t.Fatalf("GetPlacementGroupByName error: %v", err)
	}
	if pg == nil || pg.Name() != "pg-1" || pg.State() != PlacementGroupStatePrepared {
		t.Fatalf("pg = %+v", pg)
	}
}

func TestGetAllPlacementGroupsUsesGCSClient(t *testing.T) {
	jobID := ids.NewJobID()
	pgID := ids.OfPlacementGroupID(jobID)
	client := &pgMockGCSClient{
		pgList: []*proto.PlacementGroupTableData{
			{
				PlacementGroupId: pgID.Binary(),
				Name:             "pg-1",
				Strategy:         proto.PlacementStrategy_PACK,
				State:            proto.PlacementGroupTableData_CREATED,
				CreatorJobId:     jobID.Binary(),
			},
			{
				PlacementGroupId: pgID.Binary(),
				Name:             "pg-2",
				Strategy:         proto.PlacementStrategy_STRICT_SPREAD,
				State:            proto.PlacementGroupTableData_PREPARED,
			},
		},
	}
	installMockGCSClient(t, client)

	list, err := GetAllPlacementGroups(context.Background())
	if err != nil {
		t.Fatalf("GetAllPlacementGroups error: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("len = %d, want 2", len(list))
	}
	if list[0].Name() != "pg-1" || list[0].ID() != pgID || list[0].State() != PlacementGroupStateCreated || list[0].Strategy() != PlacementStrategyPack {
		t.Fatalf("list[0] = %+v", list[0])
	}
	if list[1].Name() != "pg-2" || list[1].State() != PlacementGroupStatePrepared || list[1].Strategy() != PlacementStrategyStrictSpread {
		t.Fatalf("list[1] = %+v", list[1])
	}
}

// assertRuntimeErrorState asserts err is a *errors.RuntimeError with the given
// operation and state.
func assertRuntimeErrorState(t *testing.T, err error, operation, state string) {
	t.Helper()
	re, ok := err.(*errors.RuntimeError)
	if !ok {
		t.Fatalf("expected *errors.RuntimeError, got %T: %v", err, err)
	}
	if re.Operation != operation || re.State != state {
		t.Fatalf("RuntimeError{operation: %q, state: %q}, want {%q, %q}", re.Operation, re.State, operation, state)
	}
}

func TestPlacementGroupCreationOptionsValidateNoBundles(t *testing.T) {
	if _, err := NewPlacementGroupCreationOptionsBuilder().WithName("pg-1").Build(); err == nil {
		t.Fatal("expected error for name without bundles")
	}
}

func TestCreatePlacementGroupInvalidStrategy(t *testing.T) {
	s := &pgRecordingSubmitter{}
	initPgMockRuntime(t, s)

	opts, err := NewPlacementGroupCreationOptionsBuilder().
		WithName("pg-1").
		WithBundles([]map[string]float64{{"CPU": 1}}).
		WithStrategy(PlacementStrategy(5)).
		Build()
	if err != nil {
		t.Fatalf("Build error: %v", err)
	}
	if _, err := CreatePlacementGroup(context.Background(), opts); err == nil {
		t.Fatal("expected error for invalid strategy")
	}
	if s.createCalled {
		t.Fatal("submitter should not be called for invalid strategy")
	}
}

func TestCreatePlacementGroupNilOptions(t *testing.T) {
	s := &pgRecordingSubmitter{}
	initPgMockRuntime(t, s)

	if _, err := CreatePlacementGroup(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil options")
	}
	if s.createCalled {
		t.Fatal("submitter should not be called for nil options")
	}
}

func TestCreatePlacementGroupNilSubmitter(t *testing.T) {
	initPgMockRuntime(t, nil)

	opts, err := NewPlacementGroupCreationOptionsBuilder().
		WithName("pg-1").
		WithBundles([]map[string]float64{{"CPU": 1}}).
		Build()
	if err != nil {
		t.Fatalf("Build error: %v", err)
	}
	_, err = CreatePlacementGroup(context.Background(), opts)
	assertRuntimeErrorState(t, err, "create_placement_group", "submitter_not_available")
}

func TestRemovePlacementGroupNilSubmitter(t *testing.T) {
	initPgMockRuntime(t, nil)

	err := RemovePlacementGroup(context.Background(), ids.OfPlacementGroupID(ids.NewJobID()))
	assertRuntimeErrorState(t, err, "remove_placement_group", "submitter_not_available")
}

func TestWaitPlacementGroupReadyNilSubmitter(t *testing.T) {
	initPgMockRuntime(t, nil)

	err := PlacementGroups.WaitPlacementGroupReady(context.Background(), ids.OfPlacementGroupID(ids.NewJobID()), time.Second)
	assertRuntimeErrorState(t, err, "wait_placement_group_ready", "submitter_not_available")
}

func TestGetPlacementGroupLocalModeFallback(t *testing.T) {
	// In local mode no GCS client factory is registered, so the facade must
	// fall back to the task submitter's in-process placement group store for
	// reads (Get / GetByName / GetAll). This verifies the fallback for Get by
	// id, plus the by-name and list variants.
	id := ids.OfPlacementGroupID(ids.NewJobID())
	opts := &submitter.PlacementGroupCreationOptions{
		Name:     "pg-local",
		Bundles:  []map[string]float64{{"CPU": 1}},
		Strategy: 0,
	}
	s := &pgLocalStoreSubmitter{groups: map[ids.PlacementGroupID]*submitter.PlacementGroupCreationOptions{id: opts}}
	initPgMockRuntime(t, s)

	pg, err := GetPlacementGroup(context.Background(), id)
	if err != nil {
		t.Fatalf("GetPlacementGroup (local fallback) error: %v", err)
	}
	if pg == nil {
		t.Fatal("GetPlacementGroup returned nil for a locally stored group")
	}
	if pg.ID() != id || pg.Name() != "pg-local" || len(pg.Bundles()) != 1 {
		t.Fatalf("unexpected local placement group: %+v", pg)
	}
	if pg.State() != PlacementGroupStateCreated {
		t.Fatalf("local placement group state = %v, want Created", pg.State())
	}

	// By-name lookup resolves through the local store.
	pgByName, err := GetPlacementGroupByName(context.Background(), "pg-local", "")
	if err != nil {
		t.Fatalf("GetPlacementGroupByName (local fallback) error: %v", err)
	}
	if pgByName == nil || pgByName.ID() != id {
		t.Fatalf("by-name lookup did not resolve the local group: %+v", pgByName)
	}

	// Missing group returns (nil, nil), matching the GCS-backed path.
	missing, err := GetPlacementGroup(context.Background(), ids.OfPlacementGroupID(ids.NewJobID()))
	if err != nil {
		t.Fatalf("GetPlacementGroup (missing) error: %v", err)
	}
	if missing != nil {
		t.Fatalf("expected nil for missing local group, got %+v", missing)
	}

	// List returns the stored group.
	all, err := GetAllPlacementGroups(context.Background())
	if err != nil {
		t.Fatalf("GetAllPlacementGroups (local fallback) error: %v", err)
	}
	if len(all) != 1 || all[0].ID() != id {
		t.Fatalf("unexpected local placement group list: %+v", all)
	}
}

// pgLocalStoreSubmitter is a minimal submitter that implements the api
// package's in-process placement group store (local mode), so the facade's
// GCS-less read path can be tested without a live cluster.
type pgLocalStoreSubmitter struct {
	submitter.TaskSubmitter

	groups map[ids.PlacementGroupID]*submitter.PlacementGroupCreationOptions
}

func (s *pgLocalStoreSubmitter) GetPlacementGroupLocal(ctx context.Context, id ids.PlacementGroupID) (*submitter.PlacementGroupCreationOptions, bool) {
	opts, ok := s.groups[id]
	return opts, ok
}

func (s *pgLocalStoreSubmitter) ListPlacementGroupsLocal(ctx context.Context) map[ids.PlacementGroupID]*submitter.PlacementGroupCreationOptions {
	out := make(map[ids.PlacementGroupID]*submitter.PlacementGroupCreationOptions, len(s.groups))
	for id, opts := range s.groups {
		out[id] = opts
	}
	return out
}

func TestActorWithPlacementGroupFailsLoudly(t *testing.T) {
	// Actor-level placement-group binding is unsupported on every backend, so
	// the builder must fail loudly at Create rather than silently ignoring the
	// binding. No runtime is needed: the error is recorded by WithPlacementGroup
	// and surfaced before the submitter is consulted.
	pg := &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())}
	_, err := Actor[*struct{}](&struct{}{}).WithPlacementGroup(pg, 0).Create()
	if err == nil {
		t.Fatal("expected error for actor placement-group binding, got nil")
	}
	assertRuntimeErrorState(t, err, "create_actor", "actor placement-group binding is not supported on this path")
}

func TestGetPlacementGroupFactoryNotRegistered(t *testing.T) {
	orig := getGCSClientFactory()
	t.Cleanup(func() { RegisterGCSClientFactory(orig) })
	RegisterGCSClientFactory(nil)

	_, err := GetPlacementGroup(context.Background(), ids.OfPlacementGroupID(ids.NewJobID()))
	if err != ErrGCSClientFactoryNotRegistered {
		t.Fatalf("expected ErrGCSClientFactoryNotRegistered, got %v", err)
	}
}

func TestGetPlacementGroupNotFound(t *testing.T) {
	installMockGCSClient(t, &pgMockGCSClient{pgData: nil})

	pg, err := GetPlacementGroup(context.Background(), ids.OfPlacementGroupID(ids.NewJobID()))
	if err != nil {
		t.Fatalf("expected nil error for not found, got %v", err)
	}
	if pg != nil {
		t.Fatalf("expected nil pg for not found, got %+v", pg)
	}
}

func TestGetPlacementGroupByNameNotFound(t *testing.T) {
	installMockGCSClient(t, &pgMockGCSClient{pgData: nil})

	pg, err := GetPlacementGroupByName(context.Background(), "pg-missing", "")
	if err != nil {
		t.Fatalf("expected nil error for not found, got %v", err)
	}
	if pg != nil {
		t.Fatalf("expected nil pg for not found, got %+v", pg)
	}
}

func TestGetPlacementGroupPropagatesGCSClientError(t *testing.T) {
	gcsErr := stderrors.New("gcs failure")
	installMockGCSClient(t, &pgMockGCSClient{err: gcsErr})

	if _, err := GetPlacementGroup(context.Background(), ids.OfPlacementGroupID(ids.NewJobID())); err != gcsErr {
		t.Fatalf("expected gcs error %v, got %v", gcsErr, err)
	}
	if _, err := GetPlacementGroupByName(context.Background(), "pg-1", ""); err != gcsErr {
		t.Fatalf("expected gcs error %v, got %v", gcsErr, err)
	}
	if _, err := GetAllPlacementGroups(context.Background()); err != gcsErr {
		t.Fatalf("expected gcs error %v, got %v", gcsErr, err)
	}
}
