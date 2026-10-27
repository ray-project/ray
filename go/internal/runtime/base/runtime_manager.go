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

package base

import (
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	rayerrors "github.com/ray-project/ray/go/internal/errors"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/options"
)

// shutdownHookConfig holds configuration for shutdown hook behavior.
type shutdownHookConfig struct {
	// enabled indicates whether to register shutdown hook
	enabled bool
	// signals to listen for graceful shutdown
	signals []os.Signal
}

// defaultShutdownHookConfig returns the default shutdown hook configuration.
// Shutdown hook is enabled by default to ensure graceful cleanup on process exit.
func defaultShutdownHookConfig() shutdownHookConfig {
	return shutdownHookConfig{
		enabled: true,
		signals: []os.Signal{syscall.SIGINT, syscall.SIGTERM},
	}
}

// shutdownOnce ensures shutdown hook is registered exactly once and shutdown is called exactly once.
var shutdownOnce sync.Once

// shutdownConfigLocked tracks whether shutdown hook has been registered (used to prevent config changes after registration).
var shutdownConfigLocked atomic.Bool

// workerModeInitialized tracks whether the runtime was initialized in worker
// mode (options.WorkerTypeWorker). The shutdown hook reads it to route SIGTERM
// to the graceful-exit bridge instead of the Go-side Shutdown path. Set by
// Initialize from the converted options; driver and local modes leave it false.
var workerModeInitialized atomic.Bool

// gracefulExitRequester holds the bridge that asks the C++ core worker to exit
// gracefully. The native boundary installs the real bridge (which calls the
// ray_go_worker_set_graceful_exit_requested cgo bridge) at package init; tests
// install a fake. Nil means the bridge is unavailable (no cgo layer linked),
// and the shutdown hook falls back to the plain Shutdown path. An atomic
// pointer is used so the hook reads it without locks on the signal goroutine.
var gracefulExitRequester atomic.Pointer[func()]

// SetGracefulExitRequester installs the SIGTERM graceful-exit bridge.
//
// base cannot import the cgo layer (cgo imports base), so the bridge is
// injected from the native boundary, mirroring the SetRuntimeFactory seam.
// Pass nil to uninstall (also used by tests).
func SetGracefulExitRequester(requestGracefulExit func()) {
	if requestGracefulExit == nil {
		gracefulExitRequester.Store(nil)
		return
	}
	gracefulExitRequester.Store(&requestGracefulExit)
}

// runtimeManager manages the global runtime singleton and handle registry.
//
// This struct encapsulates:
//  1. Global runtime instance for singleton enforcement
//  2. Global RWMutex for thread-safe initialization/shutdown (RWMutex allows
//     concurrent reads while ensuring exclusive writes)
//  3. Handle registry for tracking allocated handles
//  4. Shutdown hook configuration
//  5. Cached Handle instance to reduce GC pressure
//
// Note: ObjectStore is now managed internally by the Runtime, not cached here.
type runtimeManager struct {
	globalRuntime  Runtime
	globalLock     sync.RWMutex
	handleRegistry *HandleRegistry
	shutdownConfig shutdownHookConfig
	cachedHandle   RuntimeHandle
}

// manager is the global runtime manager instance.
var manager = &runtimeManager{
	handleRegistry: NewHandleRegistry(),
	shutdownConfig: defaultShutdownHookConfig(),
}

// getGlobalRuntime returns the global runtime instance.
// Returns nil if the runtime is not initialized.
// Note: This is an internal function, not exported for external use.
func getGlobalRuntime() Runtime {
	return manager.globalRuntime
}

// setGlobalRuntime sets the global runtime instance.
// This function should be called during initialization.
// Note: This is an internal function, not exported for external use.
func setGlobalRuntime(rt Runtime) {
	manager.globalRuntime = rt
}

// getGlobalLock returns the global lock for runtime initialization/shutdown.
// The lock should be held when modifying the global runtime state.
// Note: Returns RWMutex to support both read and write locks.
// Note: This is an internal function, not exported for external use.
func getGlobalLock() *sync.RWMutex {
	return &manager.globalLock
}

// getHandleRegistry returns the global handle registry.
// The registry is used to track all allocated handles for validation.
// Note: This is an internal function, not exported for external use.
func getHandleRegistry() *HandleRegistry {
	return manager.handleRegistry
}

// HandleRegistry tracks all allocated handles for validation.
// Using sync.Map for lock-free concurrent access.
type HandleRegistry struct {
	handles sync.Map // map[RuntimeHandle]struct{}
}

// NewHandleRegistry creates a new HandleRegistry instance.
func NewHandleRegistry() *HandleRegistry {
	return &HandleRegistry{}
}

// Store stores a handle in the registry.
// Handles with nil Runtime are not registered.
func (r *HandleRegistry) Store(handle RuntimeHandle) {
	if handle == nil {
		return
	}
	// Check if Runtime is nil to avoid registering invalid handles
	if handle.Runtime() == nil {
		return
	}
	r.handles.Store(handle, struct{}{})
}

// Load checks if a handle is registered in the registry.
func (r *HandleRegistry) Load(handle RuntimeHandle) bool {
	if handle == nil {
		return false
	}
	_, ok := r.handles.Load(handle)
	return ok
}

// Delete removes a handle from the registry.
func (r *HandleRegistry) Delete(handle RuntimeHandle) {
	if handle != nil {
		r.handles.Delete(handle)
	}
}

// Initialize initializes the Ray runtime and returns a handle.
//
// This function is the main entry point for initializing the Ray runtime.
// It performs the following steps:
// 1. Converts API options to internal options
// 2. Creates a Runtime instance using the factory
// 3. Starts the runtime
// 4. Creates an object store
// 5. Creates and registers a RuntimeHandle
// 6. Registers shutdown hook for graceful cleanup
//
// Thread safety: Uses global lock to ensure only one runtime instance can be created.
// The lock is held only during the singleton check and assignment, not during I/O operations.
func Initialize(opts options.InitializeOptions) (RuntimeHandle, error) {
	// Fast path: check if runtime is already initialized without acquiring lock.
	if manager.globalRuntime != nil {
		return nil, rayerrors.ErrRuntimeAlreadyInitialized
	}

	logger := log.Log
	logger.Info("Initializing Ray runtime", "options", opts)

	// Convert API options to internal options using conversion function.
	baseOpts, err := InitializeOptionsFromAPI(opts)
	if err != nil {
		logger.Error(err, "Failed to convert API options")
		return nil, err
	}

	// Record the runtime mode for the shutdown hook: in worker mode, SIGTERM
	// is routed to the graceful-exit bridge instead of the Go-side Shutdown.
	workerModeInitialized.Store(baseOpts.WorkerType == options.WorkerTypeWorker)

	// Create Runtime instance using base package (actually created by factory registered in native package).
	rt, err := CreateRuntime(baseOpts)
	if err != nil {
		logger.Error(err, "Failed to create runtime")
		return nil, err
	}

	// Start Runtime (I/O operation, no lock held).
	if err := rt.Start(); err != nil {
		// Resource cleanup: shutdown Runtime when Start fails.
		logger.Error(err, "Failed to start runtime")
		rt.Shutdown()
		return nil, err
	}
	logger.Info("Runtime started successfully")

	// Wrap as type-safe RuntimeHandle.
	// ObjectStore is now managed internally by the Runtime and accessed through
	// Runtime.GetObjectStore(). This maintains encapsulation and ensures ObjectStore
	// lifecycle is tied to Runtime lifecycle.
	handle := NewRuntimeHandle(rt)

	// Acquire global write lock only for the singleton check and registration.
	// This minimizes lock contention by excluding I/O operations from the critical section.
	manager.globalLock.Lock()
	defer manager.globalLock.Unlock()

	// Double-check: verify runtime is still not initialized after acquiring lock.
	if manager.globalRuntime != nil {
		// Another goroutine initialized first, clean up our resources.
		logger.Info("Runtime already initialized by another goroutine, cleaning up")
		manager.handleRegistry.Delete(handle)
		rt.Shutdown()
		return nil, rayerrors.ErrRuntimeAlreadyInitialized
	}

	// Register handle in registry for validation.
	manager.handleRegistry.Store(handle)

	// Store runtime for singleton enforcement.
	manager.globalRuntime = rt

	// Pre-create cached Handle for zero-overhead GetHandle() calls.
	// This avoids the need for locking or sync.Once in the hot path.
	manager.cachedHandle = handle

	// Register shutdown hook if enabled and not already registered.
	// This is done after successful initialization to ensure proper cleanup.
	if manager.shutdownConfig.enabled {
		registerShutdownHookOnce()
	}

	logger.Info("Ray runtime initialized successfully")
	return handle, nil
}

// registerShutdownHookOnce registers the shutdown hook exactly once.
// The shutdown hook listens for SIGINT and SIGTERM signals and gracefully
// shuts down the Ray runtime before process exit.
//
// Design notes:
// 1. Uses sync.Once to ensure single registration and single shutdown
// 2. Runs shutdown in a separate goroutine with repeated-signal draining (see runShutdownSignalLoop)
// 3. Logs shutdown progress for debugging
// 4. This is a global singleton shutdown mechanism, different from worker.Worker.Shutdown():
//   - worker.Worker: Per-instance shutdown, called explicitly by user code
//   - runtime_manager: Global singleton shutdown, triggered by OS signals (SIGINT/SIGTERM)
//
// Deadlock avoidance:
//   - Initialize() acquires globalLock.Lock() only briefly (line ~230) for state update,
//     NOT during I/O operations (rt.Start(), object store creation).
//   - GetHandle() uses RLock() which can proceed unless Shutdown() holds Lock().
//   - Signal handler goroutine calls GetHandle() then Shutdown(), both may wait for Lock().
//   - Since Initialize() releases Lock before I/O, and signal handler runs in separate
//     goroutine, there is no circular wait condition → no deadlock.
func registerShutdownHookOnce() {
	shutdownOnce.Do(func() {
		// Create signal channel
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, manager.shutdownConfig.signals...)

		// Start goroutine to handle shutdown signals
		go runShutdownSignalLoop(sigChan)

		shutdownConfigLocked.Store(true)
		log.Log.Info("Shutdown hook registered successfully")
	})
}

// runShutdownSignalLoop is the body of the shutdown-hook signal goroutine. It
// handles the first shutdown signal, and keeps draining the channel while the
// process is still exiting so a repeated signal can never fall through to the
// OS default disposition.
//
// Two exit shapes exist:
//
//   - Synchronous (SIGINT in any mode; SIGTERM in driver or local mode, or
//     worker mode without the graceful-exit bridge): handleShutdownSignal runs
//     the full Go-side shutdown inline, after which signal delivery is stopped
//     and the process exit proceeds.
//   - Deferred (worker-mode SIGTERM with the graceful-exit bridge): the exit
//     is only requested; the C++ core worker observes the flag at its next
//     periodic check_signals poll and exits by itself. Until that poll fires,
//     the process is alive and still delivering signals, so the loop keeps
//     receiving from the channel. A second signal in that window (raylet
//     re-kill, repeated ray stop, Kubernetes termination retry) escalates to
//     the forced shutdown path instead of the OS default handler, whose hard
//     kill is exactly the death type the graceful mechanism exists to avoid.
//     Any signal kind escalates: the process is already exiting, so a caller
//     repeating itself only means "stop waiting".
func runShutdownSignalLoop(sigChan chan os.Signal) {
	sig := <-sigChan
	log.Log.Info("Received shutdown signal", "signal", sig)

	if handleShutdownSignal(sig) {
		repeated := <-sigChan
		log.Log.Info("Repeated shutdown signal during graceful worker exit; forcing shutdown", "signal", repeated)
		runForcedShutdown()
	}

	// Stop listening for signals
	signal.Stop(sigChan)
	close(sigChan)
}

// handleShutdownSignal routes one received shutdown signal to its exit path.
// It reports whether the signal initiated a deferred graceful worker exit
// (worker-mode SIGTERM with the graceful-exit bridge installed): in that case
// the process stays alive until the core worker's poll exits it, and the
// signal goroutine must keep draining the channel (see runShutdownSignalLoop).
// Every other path completes the shutdown synchronously and reports false.
//
// Worker mode + SIGTERM is special: instead of tearing the runtime down from
// Go, the handler invokes the injected graceful-exit bridge, which flags the
// C++ core worker. The core worker's periodic check_signals poll observes the
// flag and exits with INTENDED_USER_EXIT (object flush, task wrap-up, correct
// raylet accounting); calling Go-side Shutdown here instead would race the
// core worker's own teardown and surface as a SYSTEM_ERROR worker death.
//
// Every other combination keeps the pre-existing full shutdown behavior:
// SIGINT in any mode (driver Ctrl-C semantics are unchanged), and SIGTERM in
// driver or local mode. Worker-mode SIGTERM also falls back to this path when
// no bridge is installed.
//
// Deadlock avoidance (inherited from the original inline goroutine body):
//   - GetHandle() uses RLock, Shutdown() uses Lock.
//   - Initialize() holds Lock only briefly for state update (not during I/O),
//     so a concurrent Shutdown() will wait and retry without circular waits.
//   - This function runs on the signal goroutine, never blocking Initialize().
func handleShutdownSignal(sig os.Signal) bool {
	logger := log.Log

	if sig == syscall.SIGTERM && workerModeInitialized.Load() {
		if request := gracefulExitRequester.Load(); request != nil {
			logger.Info("Worker received SIGTERM; requesting graceful core worker exit", "signal", sig)
			(*request)()
			return true
		}
		logger.Info("Graceful exit bridge unavailable; falling back to runtime shutdown", "signal", sig)
	}

	runForcedShutdown()
	return false
}

// runForcedShutdown tears the runtime down from Go. It is the pre-existing
// forced exit path, used both as the primary exit shape for signals that must
// not defer (driver modes, SIGINT) and as the escalation for a repeated signal
// arriving while a graceful worker exit is still pending.
func runForcedShutdown() {
	logger := log.Log

	logger.Info("Starting graceful shutdown")

	// Get current handle and shutdown
	handle := GetHandle()
	if handle != nil {
		if err := Shutdown(handle); err != nil {
			logger.Error(err, "Error during shutdown")
		}
	}

	logger.Info("Graceful shutdown completed")
}

// SetShutdownHookEnabled enables or disables the shutdown hook.
// This function should be called before Ray.init() to take effect.
//
// Parameters:
//   - enabled: true to enable shutdown hook (default), false to disable
//
// Note: Disabling the shutdown hook means the user is responsible for
// explicitly calling Shutdown() to clean up resources.
func SetShutdownHookEnabled(enabled bool) {
	if shutdownConfigLocked.Load() {
		log.Log.Info("Shutdown hook already registered, cannot change enabled state")
		return
	}

	manager.shutdownConfig.enabled = enabled
	log.Log.Info("Shutdown hook enabled state updated", "enabled", enabled)
}

// SetShutdownHookSignals configures which signals trigger graceful shutdown.
// This function should be called before Ray.init() to take effect.
//
// Parameters:
//   - signals: list of os.Signal to listen for (default: SIGINT, SIGTERM)
//
// Example:
//
//	core.SetShutdownHookSignals(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
func SetShutdownHookSignals(signals ...os.Signal) {
	if shutdownConfigLocked.Load() {
		log.Log.Info("Shutdown hook already registered, cannot change signals")
		return
	}

	manager.shutdownConfig.signals = signals
	log.Log.Info("Shutdown hook signals updated", "signals", signals)
}

// Shutdown shuts down the Ray runtime and cleans up resources.
//
// Thread safety: Uses global lock to ensure safe cleanup.
// The lock is held only during validation and state update, not during I/O operations.
//
// Important: After Shutdown() returns, the RuntimeHandle and its ObjectStore
// should not be used. The ObjectStore is closed along with the Runtime,
// and any subsequent access may return invalid data or cause errors.
func Shutdown(handle RuntimeHandle) error {
	logger := log.Log
	logger.Info("Shutting down Ray runtime")

	// Validate handle using encapsulated function.
	if err := ValidateHandle(handle); err != nil {
		return err
	}

	// Get runtime from handle.
	handleImpl, ok := handle.(*RuntimeHandleImpl[Runtime])
	if !ok {
		logger.Error(nil, "Invalid handle type")
		return rayerrors.ErrInvalidHandle
	}
	rt := handleImpl.Runtime()

	// Acquire global write lock to update shared state.
	// Hold lock only for state update, not for I/O operations.
	manager.globalLock.Lock()
	// Unregister handle before cleanup.
	manager.handleRegistry.Delete(handle)
	// Clear global runtime only if it matches the runtime being shut down.
	if manager.globalRuntime == rt {
		manager.globalRuntime = nil
	}
	// Clear cached handle to prevent access to shutdown resources.
	// This ensures GetHandle() returns nil after Shutdown().
	manager.cachedHandle = nil
	manager.globalLock.Unlock()

	// Perform shutdown (I/O operation, no lock held).
	if err := rt.Shutdown(); err != nil {
		logger.Error(err, "Failed to shutdown runtime")
		return err
	}

	logger.Info("Ray runtime shutdown successfully")
	return nil
}

// GetHandle returns the global runtime handle.
// Returns nil if the runtime is not initialized.
//
// Important notes:
//  1. The returned handle is the same as the one returned by Initialize().
//     It can be used for both reading runtime state and Shutdown().
//  2. This is a zero-overhead operation: the Handle is pre-created during Initialize().
//  3. The handle is safe to use for all runtime operations.
func GetHandle() RuntimeHandle {
	// Zero-overhead: just return the pre-created cached handle.
	// No locking or atomic operations needed.
	return manager.cachedHandle
}

// IsInitialized checks if the Ray runtime has been initialized.
func IsInitialized() bool {
	return manager.globalRuntime != nil
}

// ValidateHandle validates a runtime handle for use.
// It checks:
// 1. Handle is not nil
// 2. Handle is registered in the global registry
//
// This function encapsulates the validation logic to avoid exposing
// the internal registry implementation details to callers.
func ValidateHandle(handle RuntimeHandle) error {
	if handle == nil {
		return rayerrors.ErrInvalidHandle
	}
	if !manager.handleRegistry.Load(handle) {
		return rayerrors.ErrInvalidHandle
	}
	return nil
}
