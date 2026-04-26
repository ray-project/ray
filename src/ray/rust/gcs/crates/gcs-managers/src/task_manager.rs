//! GCS Task Manager -- manages task event storage and retrieval.
//!
//! Maps C++ `GcsTaskManager` from `src/ray/gcs/gcs_task_manager.h/cc`.
//!
//! ## Storage semantics (matching C++)
//!
//! - Events for the same (task_id, attempt_number) are **merged** (not replaced)
//! - **Priority-based eviction**: finished tasks (prio 0) evicted before actor
//!   tasks (prio 1) evicted before normal running tasks (prio 2). Within same
//!   priority: FIFO (oldest evicted first).
//! - **Drop tracking**: Per-job counters for dropped task attempts and profile events.
//!   These are reported in `GetTaskEventsReply`.
//! - **Data loss from workers**: `dropped_task_attempts` in `AddTaskEventDataRequest`
//!   are recorded and prevent re-ingestion.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tonic::{Request, Response, Status};
use tracing::debug;

use gcs_proto::ray::rpc::task_info_gcs_service_server::TaskInfoGcsService;
use gcs_proto::ray::rpc::*;

fn ok_status() -> GcsStatus {
    GcsStatus { code: 0, message: String::new() }
}

mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Per-job drop accounting. Mirrors C++ `JobTaskSummary`
/// (`gcs_task_manager.h:342-385`).
///
/// The total "attempts dropped" reported externally is
/// `tracked.len() + evicted` — keeping both a bounded set for
/// deduplication and a cumulative counter so GC'ing the set doesn't
/// erase the dropped-count history.
#[derive(Default)]
struct JobTaskSummary {
    /// Task attempts dropped, currently tracked for deduplication.
    /// Size is bounded by
    /// `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`
    /// via `gc_old_dropped_task_attempts`. Maps C++
    /// `dropped_task_attempts_`.
    dropped_task_attempts: HashSet<(Vec<u8>, i32)>, // (task_id, attempt_number)
    /// Number of profile events dropped. Maps C++
    /// `num_profile_events_dropped_`.
    num_profile_events_dropped: i64,
    /// Cumulative count of task attempts evicted from the tracking
    /// set by `gc_old_dropped_task_attempts`. Preserved across GC so
    /// the RPC can still report the true total. Maps C++
    /// `num_dropped_task_attempts_evicted_`.
    num_dropped_task_attempts_evicted: i64,
}

impl JobTaskSummary {
    /// Record a task attempt as dropped. Mirrors C++
    /// `JobTaskSummary::RecordTaskAttemptDropped`
    /// (`gcs_task_manager.h:347-350`).
    fn record_task_attempt_dropped(&mut self, task_id: Vec<u8>, attempt: i32) {
        self.dropped_task_attempts.insert((task_id, attempt));
    }

    /// Whether a task attempt was already dropped. Mirrors C++
    /// `ShouldDropTaskAttempt` (`gcs_task_manager.h:359-361`).
    fn should_drop_task_attempt(&self, task_id: &[u8], attempt: i32) -> bool {
        self.dropped_task_attempts
            .contains(&(task_id.to_vec(), attempt))
    }

    /// Total dropped attempts across this job's lifetime — tracked
    /// set size *plus* attempts that were evicted from tracking. The
    /// RPC surfaces this, not the tracked set size. Mirrors C++
    /// `NumTaskAttemptsDropped` (`gcs_task_manager.h:365-367`).
    fn num_task_attempts_dropped(&self) -> i64 {
        self.dropped_task_attempts.len() as i64 + self.num_dropped_task_attempts_evicted
    }

    /// Called when the job finishes: clear the tracked set so we don't
    /// retain per-job memory indefinitely. The evicted counter stays so
    /// historical totals still round-trip through `GetTaskEvents`.
    /// Mirrors C++ `OnJobEnds` (`gcs_task_manager.h:376`).
    fn on_job_ends(&mut self) {
        self.dropped_task_attempts.clear();
    }

    /// Cap the tracked set at `max_tracked` entries. When over the cap,
    /// evicts `overflow + 10% * overflow` entries (the "10% extra to
    /// mitigate thrashing" rule from C++
    /// `GcOldDroppedTaskAttempts` at `gcs_task_manager.cc:773-809`)
    /// and bumps `num_dropped_task_attempts_evicted` by that amount.
    ///
    /// `max_tracked == 0` is treated as "no cap" — same convention
    /// every other `max_*` knob in the Rust GCS uses.
    fn gc_old_dropped_task_attempts(&mut self, max_tracked: usize) {
        if max_tracked == 0 {
            return;
        }
        let len = self.dropped_task_attempts.len();
        if len <= max_tracked {
            return;
        }
        let overflow = len - max_tracked;
        // `overflow + (overflow / 10)` ≈ overflow * 1.1 rounded down.
        // `min(len, …)` clamps in case the 10% push exceeds the set.
        let to_evict = overflow
            .saturating_add(overflow / 10)
            .min(len);
        if to_evict == 0 {
            return;
        }
        // Collect the first `to_evict` elements from the iteration
        // order (HashSet gives an arbitrary order — matches C++
        // `dropped_task_attempts_.begin()` which is also
        // hash-order-dependent). This preserves the "evict ignoring
        // timestamp" comment at `gcs_task_manager.cc:804`.
        let victims: Vec<(Vec<u8>, i32)> = self
            .dropped_task_attempts
            .iter()
            .take(to_evict)
            .cloned()
            .collect();
        for v in &victims {
            self.dropped_task_attempts.remove(v);
        }
        self.num_dropped_task_attempts_evicted += to_evict as i64;
    }
}

/// GC priority: lower = evicted first.
/// Maps C++ `FinishedTaskActorTaskGcPolicy`.
fn gc_priority(task_event: &TaskEvents) -> usize {
    if is_task_finished(task_event) {
        return 0; // Finished tasks — evict first
    }
    if is_actor_task(task_event) {
        return 1; // Actor tasks — medium priority
    }
    2 // Normal/running tasks — keep longest
}

fn is_task_finished(task_event: &TaskEvents) -> bool {
    // Parity with C++ `IsTaskTerminated` (protobuf_utils.cc:314-322):
    //     state_ts_ns contains FINISHED or FAILED.
    if let Some(ref su) = task_event.state_updates {
        su.state_ts_ns
            .contains_key(&(TaskStatus::Finished as i32))
            || su.state_ts_ns.contains_key(&(TaskStatus::Failed as i32))
    } else {
        false
    }
}

fn is_actor_task(task_event: &TaskEvents) -> bool {
    task_event
        .task_info
        .as_ref()
        .map(|ti| ti.r#type == 2 || ti.r#type == 3) // ACTOR_CREATION_TASK=2, ACTOR_TASK=3
        .unwrap_or(false)
}

/// GCS Task Manager. Maps C++ `GcsTaskManager`.
///
/// Stores task events in memory with priority-based eviction and per-job
/// drop tracking.
pub struct GcsTaskManager {
    inner: RwLock<TaskEventStorage>,
    max_num_task_events: usize,
    /// Per-task cap on profile events, captured at construction from
    /// `ray_config::task_events_max_num_profile_events_per_task`
    /// (C++ `ray_config_def.h:529`, default 1000). Parity with C++
    /// `gcs_task_manager.cc:180-191`, which trims oldest events from
    /// the front on every merge and records the overflow as dropped.
    max_profile_events_per_task: usize,
    /// Optional usage-stats client. Mirrors C++ `usage_stats_client_`
    /// field + `SetUsageStatsClient` (`gcs_task_manager.h:157`,
    /// `.cc:724-727`). Installed after KV init by
    /// `GcsServer::InitUsageStatsClient` (`gcs_server.cc:636`).
    usage_stats_client: parking_lot::Mutex<Option<Arc<crate::usage_stats::UsageStatsClient>>>,
    /// Per-task-type lifetime counters. Parity with the C++ stats
    /// counters `kTotalNumActorCreationTask`, `kTotalNumActorTask`,
    /// `kTotalNumNormalTask`, `kTotalNumDriverTask`
    /// (`gcs_task_manager.h:47-58`). Bumped exactly once per
    /// `(task_id, attempt_number)` — the first observation that
    /// carries `task_info`.
    task_type_counters: parking_lot::Mutex<TaskTypeCounters>,
}

/// Mirrors the named entries in C++ `GcsTaskManagerCounter`.
#[derive(Default, Clone, Copy)]
struct TaskTypeCounters {
    actor_creation: i64,
    actor: i64,
    normal: i64,
    driver: i64,
}

struct TaskEventStorage {
    /// Priority lists: index 0 = lowest priority (evict first), 2 = highest.
    /// Each list is a VecDeque (front = newest, back = oldest).
    priority_lists: [VecDeque<TaskEvents>; 3],
    /// Primary index: (task_id, attempt_number) → (priority, position in that list).
    /// Position is tracked as an offset from the front.
    primary_index: HashMap<(Vec<u8>, i32), usize>, // key → priority bucket
    /// Per-job drop stats.
    job_summaries: HashMap<Vec<u8>, JobTaskSummary>,
    /// Total events stored (across all priority lists).
    num_stored: i64,
    /// Total task attempts dropped (eviction + worker data loss).
    /// Widened to i64 to match the per-job counters — a long-running
    /// cluster can exceed i32 across many jobs. Mirrors the value C++
    /// reports through the `kTotalNumTaskAttemptsDropped` stats counter.
    total_attempts_dropped: i64,
    /// Total profile events dropped. Same widening reason as above.
    total_profile_events_dropped: i64,
}

impl TaskEventStorage {
    fn new() -> Self {
        Self {
            priority_lists: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
            primary_index: HashMap::new(),
            job_summaries: HashMap::new(),
            num_stored: 0,
            total_attempts_dropped: 0,
            total_profile_events_dropped: 0,
        }
    }

    fn task_attempt_key(task_id: &[u8], attempt: i32) -> (Vec<u8>, i32) {
        (task_id.to_vec(), attempt)
    }

    /// Add or merge a task event. Evicts if over capacity, and trims
    /// profile events past `max_profile_events_per_task`. Returns the
    /// task type (i32) of this attempt iff this call was the *first*
    /// time the attempt became visible with `task_info` — i.e., the
    /// caller should bump the per-type usage counter exactly once.
    /// Mirrors C++ `stats_counter_.Increment(kTaskTypeToCounterType…)`
    /// at `gcs_task_manager.cc:173` (merge) + `:228` (insert).
    fn add_or_replace(
        &mut self,
        events: TaskEvents,
        max_events: usize,
        max_profile_events_per_task: usize,
    ) -> Option<i32> {
        let key = Self::task_attempt_key(&events.task_id, events.attempt_number);
        let job_id = events.job_id.clone();

        // Skip if this attempt was previously marked dropped. C++ calls
        // `ShouldDropTaskAttempt` for this same gate (`gcs_task_manager.cc`
        // not shown in the blocker excerpt but part of `RecordTaskEventData`).
        if let Some(summary) = self.job_summaries.get(&job_id) {
            if summary.should_drop_task_attempt(&events.task_id, events.attempt_number) {
                return None;
            }
        }

        let priority = gc_priority(&events);
        let mut first_observed_type: Option<i32> = None;

        if let Some(&existing_prio) = self.primary_index.get(&key) {
            // Merge into existing event.
            if let Some(existing) = self.priority_lists[existing_prio]
                .iter_mut()
                .find(|e| e.task_id == key.0 && e.attempt_number == key.1)
            {
                // Merge task_info if new one provided — and flag this
                // as the observed-for-the-first-time event for the
                // caller's usage-counter bump. Same condition C++
                // uses at `gcs_task_manager.cc:172`:
                //   `if (task_events.has_task_info() && !existing_task.has_task_info())`.
                if events.task_info.is_some() && existing.task_info.is_none() {
                    first_observed_type = events.task_info.as_ref().map(|ti| ti.r#type);
                    existing.task_info = events.task_info;
                }
                // Merge state_updates.
                if let Some(new_su) = events.state_updates {
                    if let Some(ref mut existing_su) = existing.state_updates {
                        for (k, v) in &new_su.state_ts_ns {
                            existing_su.state_ts_ns.insert(*k, *v);
                        }
                        if new_su.node_id.is_some() {
                            existing_su.node_id = new_su.node_id;
                        }
                        if new_su.worker_id.is_some() {
                            existing_su.worker_id = new_su.worker_id;
                        }
                        if new_su.worker_pid.is_some() {
                            existing_su.worker_pid = new_su.worker_pid;
                        }
                        if new_su.error_info.is_some() {
                            existing_su.error_info = new_su.error_info;
                        }
                        if new_su.is_debugger_paused.is_some() {
                            existing_su.is_debugger_paused = new_su.is_debugger_paused;
                        }
                        if new_su.actor_repr_name.is_some() {
                            existing_su.actor_repr_name = new_su.actor_repr_name;
                        }
                    } else {
                        existing.state_updates = Some(new_su);
                    }
                }
                // Merge profile events.
                if let Some(new_pe) = events.profile_events {
                    if let Some(ref mut existing_pe) = existing.profile_events {
                        existing_pe.events.extend(new_pe.events);
                    } else {
                        existing.profile_events = Some(new_pe);
                    }
                }

                // Truncate profile events past the per-task cap. Parity
                // with C++ `gcs_task_manager.cc:179-191` — drop the
                // oldest (front) entries and record the overflow both
                // per-job and globally so `GetTaskEvents` reports it.
                let dropped_profile = Self::truncate_profile_events(
                    existing,
                    max_profile_events_per_task,
                );
                if dropped_profile > 0 {
                    let n = dropped_profile as i64;
                    self.job_summaries.entry(job_id.clone()).or_default()
                        .num_profile_events_dropped += n;
                    self.total_profile_events_dropped += n;
                }

                // Update priority if task state changed (e.g., became finished).
                let new_prio = gc_priority(existing);
                if new_prio != existing_prio {
                    // Move to correct priority list.
                    let task = self.priority_lists[existing_prio]
                        .iter()
                        .position(|e| e.task_id == key.0 && e.attempt_number == key.1);
                    if let Some(pos) = task {
                        let removed = self.priority_lists[existing_prio].remove(pos).unwrap();
                        self.priority_lists[new_prio].push_front(removed);
                        self.primary_index.insert(key, new_prio);
                    }
                }
            }
        } else {
            // New task event — truncate profile events before insert so
            // a single over-large batch can't bypass the cap.
            let mut events = events;
            let dropped_profile = Self::truncate_profile_events(
                &mut events,
                max_profile_events_per_task,
            );
            if dropped_profile > 0 {
                let n = dropped_profile as i64;
                self.job_summaries.entry(job_id.clone()).or_default()
                    .num_profile_events_dropped += n;
                self.total_profile_events_dropped += n;
            }

            // First observation of this attempt — if the initial event
            // carries `task_info`, the per-type counter should be
            // bumped exactly once. Parity with C++ insert-side bump at
            // `gcs_task_manager.cc:228`.
            if let Some(ref ti) = events.task_info {
                first_observed_type = Some(ti.r#type);
            }

            self.priority_lists[priority].push_front(events);
            self.primary_index.insert(key, priority);
            self.num_stored += 1;

            // Evict if over capacity.
            if max_events > 0 && self.num_stored as usize > max_events {
                self.evict_one();
            }
        }

        first_observed_type
    }

    /// Trim profile events from the front so the list fits within
    /// `max_profile_events_per_task`. Returns the number dropped.
    /// Parity with C++ `gcs_task_manager.cc:179-191`:
    ///
    /// > existing_task.mutable_profile_events()->mutable_events()
    /// >     ->DeleteSubrange(0, to_drop);
    ///
    /// Oldest-first drop, same direction as C++. A cap of 0 is treated
    /// as "no truncation" to match the existing "0 disables" convention
    /// used by other config knobs (health loop, debug dump, etc.).
    fn truncate_profile_events(event: &mut TaskEvents, cap: usize) -> i32 {
        if cap == 0 {
            return 0;
        }
        let Some(ref mut pe) = event.profile_events else {
            return 0;
        };
        if pe.events.len() <= cap {
            return 0;
        }
        let to_drop = pe.events.len() - cap;
        pe.events.drain(0..to_drop);
        to_drop as i32
    }

    /// Evict the lowest-priority, oldest event.
    fn evict_one(&mut self) {
        for prio in 0..3 {
            if let Some(evicted) = self.priority_lists[prio].pop_back() {
                let key =
                    Self::task_attempt_key(&evicted.task_id, evicted.attempt_number);
                self.primary_index.remove(&key);
                self.num_stored -= 1;

                // Record as dropped through the summary helper so the
                // tracked/evicted accounting stays consistent.
                let job_id = evicted.job_id.clone();
                self.job_summaries
                    .entry(job_id)
                    .or_default()
                    .record_task_attempt_dropped(evicted.task_id, evicted.attempt_number);
                self.total_attempts_dropped += 1;
                return;
            }
        }
    }

    /// Record data loss from workers (dropped task attempts and profile events).
    fn record_data_loss(&mut self, data: &TaskEventData) {
        for dropped in &data.dropped_task_attempts {
            let job_id = dropped.task_id.get(..4).unwrap_or(&dropped.task_id).to_vec();
            self.job_summaries
                .entry(job_id)
                .or_default()
                .record_task_attempt_dropped(
                    dropped.task_id.clone(),
                    dropped.attempt_number,
                );
            self.total_attempts_dropped += 1;

            // Remove from storage if present.
            let key = Self::task_attempt_key(&dropped.task_id, dropped.attempt_number);
            if let Some(prio) = self.primary_index.remove(&key) {
                if let Some(pos) = self.priority_lists[prio]
                    .iter()
                    .position(|e| e.task_id == key.0 && e.attempt_number == key.1)
                {
                    self.priority_lists[prio].remove(pos);
                    self.num_stored -= 1;
                }
            }
        }

        if data.num_profile_events_dropped > 0 {
            let dropped = data.num_profile_events_dropped as i64;
            self.job_summaries
                .entry(data.job_id.clone())
                .or_default()
                .num_profile_events_dropped += dropped;
            self.total_profile_events_dropped += dropped;
        }
    }

    /// Get all events as a flat vec.
    fn all_events(&self) -> Vec<TaskEvents> {
        let mut result = Vec::with_capacity(self.num_stored as usize);
        // Higher priority first (2, 1, 0).
        for prio in (0..3).rev() {
            for event in &self.priority_lists[prio] {
                result.push(event.clone());
            }
        }
        result
    }

    /// Get events for a specific job.
    fn events_for_job(&self, job_id: &[u8]) -> Vec<TaskEvents> {
        let mut result = Vec::new();
        for prio in (0..3).rev() {
            for event in &self.priority_lists[prio] {
                if event.job_id == job_id {
                    result.push(event.clone());
                }
            }
        }
        result
    }

    /// Finalize per-job drop accounting when a job finishes. Clears
    /// the tracked dropped-attempt set so memory does not grow
    /// unboundedly as jobs retire, but preserves
    /// `num_dropped_task_attempts_evicted` so historical counts still
    /// surface through `GetTaskEvents`. Mirrors C++
    /// `GcsTaskManagerStorage::UpdateJobSummaryOnJobDone`
    /// (`gcs_task_manager.h:255-261`), which calls `OnJobEnds()` on the
    /// per-job `JobTaskSummary`.
    fn update_job_summary_on_job_done(&mut self, job_id: &[u8]) {
        if let Some(summary) = self.job_summaries.get_mut(job_id) {
            summary.on_job_ends();
        }
    }

    /// GC every job's tracked dropped-attempt set down to `max_tracked`.
    /// Mirrors C++ `GcsTaskManagerStorage::GcJobSummary`
    /// (`gcs_task_manager.h:263-267`), driven on the C++ side by a 5 s
    /// periodic task (`gcs_task_manager.cc:50-52`).
    fn gc_job_summary(&mut self, max_tracked: usize) {
        for summary in self.job_summaries.values_mut() {
            summary.gc_old_dropped_task_attempts(max_tracked);
        }
    }

    /// Get every stored event across all attempts whose `task_id` falls
    /// in the given set. Mirrors C++
    /// `GcsTaskManagerStorage::GetTaskEvents(const absl::flat_hash_set<TaskID>&)`
    /// (`gcs_task_manager.cc:81-93`), which the handler uses when a
    /// `task_filters` EQUAL predicate is present.
    fn events_for_task_ids(&self, task_ids: &HashSet<Vec<u8>>) -> Vec<TaskEvents> {
        let mut result = Vec::new();
        for prio in (0..3).rev() {
            for event in &self.priority_lists[prio] {
                if task_ids.contains(&event.task_id) {
                    result.push(event.clone());
                }
            }
        }
        result
    }

    /// Mark a single task event as FAILED if not already terminated.
    ///
    /// Maps C++ `GcsTaskManagerStorage::MarkTaskAttemptFailedIfNeeded`
    /// (gcs_task_manager.cc:129-144).
    fn mark_failed_if_needed(event: &mut TaskEvents, failed_ts_ns: i64, error: &RayErrorInfo) {
        if is_task_finished(event) {
            return;
        }
        let su = event.state_updates.get_or_insert_with(TaskStateUpdate::default);
        // TaskStatus::Failed (12) — the terminal failure state.
        su.state_ts_ns.insert(TaskStatus::Failed as i32, failed_ts_ns);
        su.error_info = Some(error.clone());
    }

    /// Mark every non-terminal task attempt for the given worker as FAILED
    /// with the supplied error.
    ///
    /// Maps C++ `GcsTaskManagerStorage::MarkTasksFailedOnWorkerDead`
    /// (gcs_task_manager.cc:107-127).
    fn mark_tasks_failed_on_worker_dead(
        &mut self,
        worker_id: &[u8],
        failed_ts_ns: i64,
        error: &RayErrorInfo,
    ) {
        for prio in 0..3 {
            for event in self.priority_lists[prio].iter_mut() {
                // Task belongs to this worker iff its state_updates.worker_id
                // matches — this is set when the scheduler picks the worker.
                let matches = event
                    .state_updates
                    .as_ref()
                    .and_then(|su| su.worker_id.as_deref())
                    .map(|w| w == worker_id)
                    .unwrap_or(false);
                if matches {
                    Self::mark_failed_if_needed(event, failed_ts_ns, error);
                }
            }
        }
    }

    /// Mark every non-terminal task attempt in the given job as FAILED.
    ///
    /// Maps C++ `GcsTaskManagerStorage::MarkTasksFailedOnJobEnds`
    /// (gcs_task_manager.cc:146-165).
    fn mark_tasks_failed_on_job_ends(
        &mut self,
        job_id: &[u8],
        failed_ts_ns: i64,
        error: &RayErrorInfo,
    ) {
        for prio in 0..3 {
            for event in self.priority_lists[prio].iter_mut() {
                if event.job_id == job_id {
                    Self::mark_failed_if_needed(event, failed_ts_ns, error);
                }
            }
        }
    }
}

impl GcsTaskManager {
    /// Construct with an explicit cap on stored task attempts and the
    /// default profile-event cap from `ray_config`.
    ///
    /// Production wiring uses `new_from_ray_config` so both knobs come
    /// from `RayConfig`. Tests that need to pin the storage cap without
    /// touching `ray_config` global state use this constructor.
    pub fn new(max_num_task_events: usize) -> Self {
        let max_profile_events_per_task = ray_config::instance()
            .task_events_max_num_profile_events_per_task
            .max(0) as usize;
        Self::new_with_limits(max_num_task_events, max_profile_events_per_task)
    }

    /// Construct with both caps explicit. Used by `new_from_ray_config`
    /// and by tests that need both dials.
    pub fn new_with_limits(
        max_num_task_events: usize,
        max_profile_events_per_task: usize,
    ) -> Self {
        Self {
            inner: RwLock::new(TaskEventStorage::new()),
            max_num_task_events,
            max_profile_events_per_task,
            usage_stats_client: parking_lot::Mutex::new(None),
            task_type_counters: parking_lot::Mutex::new(TaskTypeCounters::default()),
        }
    }

    /// Install the usage-stats recorder. Mirrors C++
    /// `GcsTaskManager::SetUsageStatsClient`
    /// (`gcs_task_manager.cc:724-727`).
    pub fn set_usage_stats_client(
        &self,
        client: Arc<crate::usage_stats::UsageStatsClient>,
    ) {
        *self.usage_stats_client.lock() = Some(client);
    }

    /// Record a newly-observed `(task_id, attempt)` with `task_info`
    /// set. Increments the per-type counter and forwards the new
    /// monotonic value to the usage-stats client. Parity with C++
    /// `stats_counter_.Increment(kTaskTypeToCounterType.at(...))` at
    /// `gcs_task_manager.cc:173` + `:228`, paired with the
    /// periodic write in `RecordMetrics` (`:711-720`).
    fn record_task_type_observed(&self, task_type: i32) {
        let (tag, new_count) = {
            let mut c = self.task_type_counters.lock();
            if task_type == TaskType::ActorCreationTask as i32 {
                c.actor_creation += 1;
                (crate::usage_stats::TagKey::NumActorCreationTasks, c.actor_creation)
            } else if task_type == TaskType::ActorTask as i32 {
                c.actor += 1;
                (crate::usage_stats::TagKey::NumActorTasks, c.actor)
            } else if task_type == TaskType::NormalTask as i32 {
                c.normal += 1;
                (crate::usage_stats::TagKey::NumNormalTasks, c.normal)
            } else if task_type == TaskType::DriverTask as i32 {
                c.driver += 1;
                (crate::usage_stats::TagKey::NumDrivers, c.driver)
            } else {
                return; // unknown task type — C++'s map lookup would abort
            }
        };
        let client = self.usage_stats_client.lock().clone();
        if let Some(client) = client {
            client.record_extra_usage_counter_spawn(tag, new_count);
        }
    }

    /// Pull both caps from the current `RayConfig` snapshot. This is
    /// the production entry point. Mirrors C++
    /// `GcsTaskManager::GcsTaskManager(size_t max_num_task_events, ...)`
    /// where `max_num_task_events` comes from
    /// `RayConfig::instance().task_events_max_num_task_in_gcs()`
    /// (`gcs_task_manager.cc:42-45`) and the profile-event cap is read
    /// inside the merge path (`gcs_task_manager.cc:180-181`). Both come
    /// from one snapshot so a burst of ingestion can't see half-and-half
    /// reconfiguration.
    pub fn new_from_ray_config() -> Self {
        let cfg = ray_config::snapshot();
        let max_num_task_events = cfg.task_events_max_num_task_in_gcs.max(0) as usize;
        let max_profile = cfg.task_events_max_num_profile_events_per_task.max(0) as usize;
        Self::new_with_limits(max_num_task_events, max_profile)
    }

    /// Mark all non-terminal task attempts running on `worker_id` as FAILED.
    ///
    /// Maps C++ `GcsTaskManager::OnWorkerDead` (gcs_task_manager.cc:729-748).
    /// C++ defers the mark behind a `gcs_mark_task_failed_on_worker_dead_delay_ms`
    /// timer; we accept the same delay so late-arriving events can still race
    /// (e.g. a task event reporting FINISHED that beats the timer wins).
    pub fn on_worker_dead(
        self: &std::sync::Arc<Self>,
        worker_id: Vec<u8>,
        worker_data: WorkerTableData,
        delay_ms: u64,
    ) {
        let exit_type = worker_data.exit_type.unwrap_or(0);
        let exit_detail = worker_data.exit_detail.clone().unwrap_or_default();
        let end_time_ms = worker_data.end_time_ms;
        let this = self.clone();
        tokio::spawn(async move {
            if delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            let error = RayErrorInfo {
                error_type: ErrorType::WorkerDied as i32,
                error_message: format!(
                    "Worker running the task ({}) died with exit_type: {} with error_message: {}",
                    hex::encode(&worker_id),
                    exit_type,
                    exit_detail,
                ),
                ..Default::default()
            };
            // C++ multiplies end_time_ms by 1_000_000 to convert to ns.
            let failed_ts_ns = end_time_ms as i64 * 1_000_000;
            let mut storage = this.inner.write();
            storage.mark_tasks_failed_on_worker_dead(&worker_id, failed_ts_ns, &error);
            debug!(
                worker_id = hex::encode(&worker_id),
                "Marked tasks failed for dead worker"
            );
        });
    }

    /// Mark all non-terminal task attempts in `job_id` as FAILED.
    ///
    /// Maps C++ `GcsTaskManager::OnJobFinished` (gcs_task_manager.cc:750-772).
    pub fn on_job_finished(
        self: &std::sync::Arc<Self>,
        job_id: Vec<u8>,
        job_finish_time_ms: i64,
        delay_ms: u64,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            if delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            let error = RayErrorInfo {
                error_type: ErrorType::WorkerDied as i32,
                error_message: format!(
                    "Job finishes ({}) as driver exits. Marking all non-terminal tasks as failed.",
                    hex::encode(&job_id),
                ),
                ..Default::default()
            };
            // C++ multiplies by 1_000_000 (ms → ns).
            let failed_ts_ns = job_finish_time_ms * 1_000_000;
            let mut storage = this.inner.write();
            storage.mark_tasks_failed_on_job_ends(&job_id, failed_ts_ns, &error);
            // Finalize the per-job summary — clear the tracked
            // dropped-attempt set (historical evicted count is
            // preserved). Parity with C++ `OnJobFinished` at
            // `gcs_task_manager.cc:768-769`:
            // `task_event_storage_->UpdateJobSummaryOnJobDone(job_id);`.
            storage.update_job_summary_on_job_done(&job_id);
            debug!(job_id = hex::encode(&job_id), "Marked tasks failed for finished job");
        });
    }

    /// Run one pass of the per-job dropped-attempt GC, reading the
    /// current cap from `ray_config`. Exposed so the server's periodic
    /// loop and tests share one entry point.
    ///
    /// Parity with C++ `GcJobSummary` invocation at
    /// `gcs_task_manager.cc:50-52` (the 5 s periodic runner).
    pub fn gc_job_summary(&self) {
        let cap = ray_config::instance()
            .task_events_max_dropped_task_attempts_tracked_per_job_in_gcs
            .max(0) as usize;
        let mut storage = self.inner.write();
        storage.gc_job_summary(cap);
    }
}

#[tonic::async_trait]
impl TaskInfoGcsService for GcsTaskManager {
    /// Add task event data — merge events and track data loss.
    /// Maps C++ `HandleAddTaskEventData` + `RecordTaskEventData`
    /// (gcs_task_manager.cc:649-666).
    async fn add_task_event_data(
        &self,
        request: Request<AddTaskEventDataRequest>,
    ) -> Result<Response<AddTaskEventDataReply>, Status> {
        let req = request.into_inner();
        // Accumulate the task-type bumps outside the storage lock so we
        // can call into the usage-stats client (which schedules on
        // tokio) without holding a write guard across an await point.
        let mut observed_types: Vec<i32> = Vec::new();

        if let Some(data) = req.data {
            let mut storage = self.inner.write();

            // Record data loss from worker first (matching C++ order).
            storage.record_data_loss(&data);

            // Store each event, trimming profile events to the per-task
            // cap and evicting oldest rows when we exceed the table cap.
            for events in data.events_by_task {
                if let Some(t) = storage.add_or_replace(
                    events,
                    self.max_num_task_events,
                    self.max_profile_events_per_task,
                ) {
                    observed_types.push(t);
                }
            }
        }

        // Outside the write lock — bump usage counters once per
        // newly-observed task_info (mirrors C++'s stats_counter bumps
        // at `gcs_task_manager.cc:173` and `:228`, then the periodic
        // write in `RecordMetrics`).
        for t in observed_types {
            self.record_task_type_observed(t);
        }

        Ok(Response::new(AddTaskEventDataReply {
            status: Some(ok_status()),
        }))
    }

    /// Get task events with filtering, limiting, and accurate stats.
    ///
    /// Faithful port of C++ `HandleGetTaskEvents`
    /// (`gcs_task_manager.cc:426-621`). Order of operations:
    ///
    ///   1. **Index preselection** — if `task_filters` has any EQUAL
    ///      predicates, use the task-id index. Else if `job_filters` has
    ///      any, use the job-id index. Else scan everything. A preselect
    ///      by *multiple* distinct task/job ids with EQUAL predicates
    ///      short-circuits to an empty set (matches C++ lines 443-447
    ///      and 466-468 — C++ treats it as "no single id satisfies them
    ///      all"; the full predicate path below would filter it down to
    ///      empty anyway, but the short-circuit avoids a linear scan).
    ///   2. **Per-event filter** — applied in the order C++ uses. The
    ///      first gate is `has_task_info`: events without task_info are
    ///      dropped unconditionally (C++ lines 491-495). This matters
    ///      because lifecycle / profile-only events can arrive before
    ///      the task-definition event and would otherwise leak through
    ///      the other filters.
    ///   3. **Malformed-predicate handling** — if a filter carries a
    ///      `predicate` value outside `{EQUAL, NOT_EQUAL}`, the reply is
    ///      cleared and the RPC returns `Status::InvalidArgument`
    ///      (parity with C++ try/catch at lines 584-618).
    async fn get_task_events(
        &self,
        request: Request<GetTaskEventsRequest>,
    ) -> Result<Response<GetTaskEventsReply>, Status> {
        let req = request.into_inner();
        let storage = self.inner.read();

        // Validate every predicate up-front; this mirrors C++'s
        // `try { … } catch (std::invalid_argument &)` scope where any
        // unknown predicate from `apply_predicate` / `apply_predicate_ignore_case`
        // triggers a clean failure, not partial results. Doing the
        // validation once at the top avoids reading a partial reply on
        // the failure path and makes the invariant local to this block.
        if let Err(msg) = validate_predicates(req.filters.as_ref()) {
            return Err(Status::invalid_argument(msg));
        }

        let limit = req
            .limit
            .filter(|&l| l > 0)
            .map(|l| l as usize)
            .unwrap_or(usize::MAX);

        // Stage 1: Index-based selection.
        //
        // i64 to accommodate long-lived clusters; reply counters are
        // truncated to i32 at serialization with `saturating_as` so
        // overflow surfaces as INT_MAX rather than wrapping.
        let mut drop_profile: i64 = 0;
        let mut drop_status: i64 = 0;

        // Collect EQUAL task-ids and job-ids. C++ uses task_filters
        // preferentially over job_filters (lines 436-468).
        let task_id_equals: HashSet<Vec<u8>> = req
            .filters
            .as_ref()
            .map(|f| {
                f.task_filters
                    .iter()
                    .filter(|tf| tf.predicate == FilterPredicate::Equal as i32)
                    .map(|tf| tf.task_id.clone())
                    .collect()
            })
            .unwrap_or_default();

        let job_id_equals: HashSet<Vec<u8>> = req
            .filters
            .as_ref()
            .map(|f| {
                f.job_filters
                    .iter()
                    .filter(|jf| jf.predicate == FilterPredicate::Equal as i32)
                    .map(|jf| jf.job_id.clone())
                    .collect()
            })
            .unwrap_or_default();

        let candidates: Vec<TaskEvents> = if !task_id_equals.is_empty() {
            // task_filters with EQUAL preselection (C++ lines 436-447).
            if task_id_equals.len() == 1 {
                storage.events_for_task_ids(&task_id_equals)
            } else {
                // Multiple distinct EQUAL task-ids cannot all be true
                // simultaneously — C++ returns an empty vec (line 446).
                Vec::new()
            }
        } else if !job_id_equals.is_empty() {
            // job_filters with EQUAL preselection (C++ lines 448-468).
            if job_id_equals.len() == 1 {
                let jid = job_id_equals.iter().next().unwrap();
                // Per-job drop stats (C++ lines 461-465). Mirror C++
                // `GetJobTaskSummary(...).NumTaskAttemptsDropped()` at
                // `gcs_task_manager.h:365-367` — tracked + evicted.
                // The tracked-set-size-only read the Rust code used
                // before would report stale (smaller) numbers after
                // `gc_old_dropped_task_attempts` or `on_job_ends`.
                if let Some(summary) = storage.job_summaries.get(jid) {
                    drop_profile = summary.num_profile_events_dropped;
                    drop_status = summary.num_task_attempts_dropped();
                }
                storage.events_for_job(jid)
            } else {
                Vec::new()
            }
        } else {
            // No index-friendly preselection — scan everything and
            // report cluster-wide drop stats (C++ lines 471-478).
            //
            // Cluster-wide counters stay as running sums; they do not
            // decrement on GC or job-done, so they match
            // `sum_over_jobs(num_task_attempts_dropped())` at all times.
            drop_profile = storage.total_profile_events_dropped;
            drop_status = storage.total_attempts_dropped;
            storage.all_events()
        };

        let num_total_stored = candidates.len() as i64;

        // Stage 2: Per-event filter. Full-predicate path — applies the
        // same check sequence as C++ lines 491-582. `filter_fn` returns
        // `false` if the event should be dropped.
        let exclude_driver = req
            .filters
            .as_ref()
            .and_then(|f| f.exclude_driver)
            .unwrap_or(false);

        let filter_fn = |event: &TaskEvents| -> bool {
            // C++ lines 492-495 — skip events without task_info. This
            // drops lifecycle/profile-only events that arrive before
            // the task-definition event has merged in.
            let Some(ti) = event.task_info.as_ref() else {
                return false;
            };

            // exclude_driver (C++ lines 496-499). DRIVER_TASK = 1.
            if exclude_driver && ti.r#type == TaskType::DriverTask as i32 {
                return false;
            }

            let Some(ref filters) = req.filters else {
                return true;
            };

            // task_filters (C++ lines 501-512).
            for tf in &filters.task_filters {
                if !apply_predicate_bytes(
                    &event.task_id,
                    tf.predicate,
                    &tf.task_id,
                ) {
                    return false;
                }
            }

            // job_filters (C++ lines 514-525). Uses task_info.job_id —
            // the enclosing proto field may be missing on legacy events,
            // so fall back to the top-level event.job_id for parity.
            let event_job = if !ti.job_id.is_empty() {
                &ti.job_id
            } else {
                &event.job_id
            };
            for jf in &filters.job_filters {
                if !apply_predicate_bytes(event_job, jf.predicate, &jf.job_id) {
                    return false;
                }
            }

            // actor_filters (C++ lines 527-538).
            for af in &filters.actor_filters {
                let actor_id: &[u8] = ti.actor_id.as_deref().unwrap_or(&[]);
                if !apply_predicate_bytes(actor_id, af.predicate, &af.actor_id) {
                    return false;
                }
            }

            // task_name_filters (C++ lines 540-550). Case-insensitive.
            for tnf in &filters.task_name_filters {
                if !apply_predicate_ignore_case(&ti.name, tnf.predicate, &tnf.task_name) {
                    return false;
                }
            }

            // state_filters (C++ lines 552-579).
            if !filters.state_filters.is_empty() {
                let current_state = latest_task_state(event);
                let current_name = TaskStatus::try_from(current_state)
                    .map(|s| s.as_str_name().to_string())
                    .unwrap_or_default();
                for sf in &filters.state_filters {
                    if !apply_predicate_ignore_case(
                        &current_name,
                        sf.predicate,
                        &sf.state,
                    ) {
                        return false;
                    }
                }
            }

            true
        };

        // Stage 3: Apply filter, limit, tally truncation. C++ walks the
        // candidates in REVERSE (line 587: `| boost::adaptors::reversed`)
        // so the newest events are reported first under a truncating
        // limit. Rust's `all_events` / `events_for_job` already return
        // highest-priority-first and FIFO per priority (front = newest);
        // to match C++ we iterate in reverse so the *newest* events
        // inside each priority bucket land first.
        let mut events_out = Vec::new();
        let mut num_filtered_on_gcs: i64 = 0;
        let mut num_truncated: i64 = 0;
        let mut truncated_profile: i64 = 0;
        let mut truncated_status: i64 = 0;

        for event in candidates.iter().rev() {
            if !filter_fn(event) {
                num_filtered_on_gcs += 1;
                continue;
            }
            if events_out.len() < limit {
                events_out.push(event.clone());
            } else {
                num_truncated += 1;
                if let Some(ref pe) = event.profile_events {
                    truncated_profile += pe.events.len() as i64;
                }
                if event.state_updates.is_some() {
                    truncated_status += 1;
                }
            }
        }

        // Reply counters are i32 on the wire; saturate rather than wrap
        // if a long-lived cluster pushes a job past INT_MAX — unlikely
        // in practice but cheap to get right.
        let sat_i32 = |v: i64| -> i32 {
            if v > i32::MAX as i64 { i32::MAX } else { v as i32 }
        };

        Ok(Response::new(GetTaskEventsReply {
            status: Some(ok_status()),
            events_by_task: events_out,
            num_profile_task_events_dropped: sat_i32(drop_profile + truncated_profile),
            num_status_task_events_dropped: sat_i32(drop_status + truncated_status),
            num_total_stored,
            num_filtered_on_gcs,
            num_truncated,
        }))
    }
}

/// Validate every filter's predicate up-front. Returns an error string
/// on the first unknown predicate, matching the C++ exception the `try`
/// block catches at `gcs_task_manager.cc:586-617`.
fn latest_task_state(event: &TaskEvents) -> i32 {
    // C++ walks TaskStatus_descriptor backward and picks the last
    // state whose timestamp was recorded (lines 557-567). We mirror
    // that: choose the key with the largest state-enum value.
    event
        .state_updates
        .as_ref()
        .and_then(|su| su.state_ts_ns.keys().max().copied())
        .unwrap_or(TaskStatus::Nil as i32)
}

fn validate_predicates(filters: Option<&get_task_events_request::Filters>) -> Result<(), String> {
    let Some(f) = filters else {
        return Ok(());
    };
    for tf in &f.task_filters {
        check_predicate(tf.predicate)?;
    }
    for jf in &f.job_filters {
        check_predicate(jf.predicate)?;
    }
    for af in &f.actor_filters {
        check_predicate(af.predicate)?;
    }
    for tnf in &f.task_name_filters {
        check_predicate(tnf.predicate)?;
    }
    for sf in &f.state_filters {
        check_predicate(sf.predicate)?;
    }
    Ok(())
}

fn check_predicate(p: i32) -> Result<(), String> {
    match FilterPredicate::try_from(p) {
        Ok(FilterPredicate::Equal) | Ok(FilterPredicate::NotEqual) => Ok(()),
        _ => Err(format!(
            "Unknown filter predicate: {p}. Supported predicates are '=' and '!='."
        )),
    }
}

/// Apply EQUAL / NOT_EQUAL to raw bytes (task_id, job_id, actor_id).
/// Mirrors C++ `apply_predicate<T>` at `gcs_task_manager.cc:394-406`.
/// Precondition: `predicate` has already passed `validate_predicates`.
fn apply_predicate_bytes(lhs: &[u8], predicate: i32, rhs: &[u8]) -> bool {
    match FilterPredicate::try_from(predicate) {
        Ok(FilterPredicate::Equal) => lhs == rhs,
        Ok(FilterPredicate::NotEqual) => lhs != rhs,
        _ => false, // validated above, unreachable in practice
    }
}

/// Apply EQUAL / NOT_EQUAL to strings, case-insensitive. Mirrors C++
/// `apply_predicate_ignore_case` (`gcs_task_manager.cc:408-422`).
fn apply_predicate_ignore_case(lhs: &str, predicate: i32, rhs: &str) -> bool {
    match FilterPredicate::try_from(predicate) {
        Ok(FilterPredicate::Equal) => lhs.eq_ignore_ascii_case(rhs),
        Ok(FilterPredicate::NotEqual) => !lhs.eq_ignore_ascii_case(rhs),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task_events(task_id: &[u8], attempt: i32, job_id: &[u8]) -> TaskEvents {
        // Always populate task_info — `get_task_events` correctly drops
        // events without it (parity with C++ `gcs_task_manager.cc:491-495`
        // / blocker 1b). Real clients set task_info when they emit the
        // task-definition event, so this fixture reflects the
        // client-facing contract the RPC expects.
        TaskEvents {
            task_id: task_id.to_vec(),
            attempt_number: attempt,
            job_id: job_id.to_vec(),
            task_info: Some(TaskInfoEntry {
                job_id: job_id.to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Variant that deliberately omits `task_info` — only used by the
    /// skip-no-task_info parity test.
    fn make_task_events_no_info(task_id: &[u8], attempt: i32, job_id: &[u8]) -> TaskEvents {
        TaskEvents {
            task_id: task_id.to_vec(),
            attempt_number: attempt,
            job_id: job_id.to_vec(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_add_and_get_task_events() {
        let mgr = GcsTaskManager::new(1000);

        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![make_task_events(b"task1", 0, b"job1")],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.events_by_task.len(), 1);
        assert_eq!(reply.events_by_task[0].task_id, b"task1");
        assert_eq!(reply.num_total_stored, 1);
    }

    #[tokio::test]
    async fn test_eviction_priority() {
        // Finished tasks should be evicted before running tasks.
        let mgr = GcsTaskManager::new(2);

        // Add a running task.
        let mut running = make_task_events(b"running", 0, b"j1");
        running.task_info = Some(TaskInfoEntry {
            r#type: 0, // NORMAL_TASK
            ..Default::default()
        });

        // Add a finished task.
        let mut finished = make_task_events(b"finished", 0, b"j1");
        finished.state_updates = Some(TaskStateUpdate {
            state_ts_ns: [(TaskStatus::Finished as i32, 1000)].into_iter().collect(),
            ..Default::default()
        });

        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![running, finished],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        // Add a third event to trigger eviction (capacity=2).
        let third = make_task_events(b"third", 0, b"j1");
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![third],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        // Finished task should have been evicted (priority 0).
        assert_eq!(reply.events_by_task.len(), 2);
        let ids: Vec<_> = reply
            .events_by_task
            .iter()
            .map(|e| e.task_id.clone())
            .collect();
        assert!(!ids.contains(&b"finished".to_vec()));
        assert!(ids.contains(&b"running".to_vec()));
        assert!(ids.contains(&b"third".to_vec()));
    }

    #[tokio::test]
    async fn test_event_merging() {
        let mgr = GcsTaskManager::new(1000);

        // Add task_info.
        let mut e1 = make_task_events(b"t1", 0, b"j1");
        e1.task_info = Some(TaskInfoEntry {
            name: "my_task".to_string(),
            ..Default::default()
        });

        // Add state_update for same (task_id, attempt).
        let mut e2 = make_task_events(b"t1", 0, b"j1");
        e2.state_updates = Some(TaskStateUpdate {
            node_id: Some(b"node1".to_vec()),
            ..Default::default()
        });

        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![e1, e2],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        // Should be merged into single event.
        assert_eq!(reply.events_by_task.len(), 1);
        assert!(reply.events_by_task[0].task_info.is_some());
        assert!(reply.events_by_task[0].state_updates.is_some());
        assert_eq!(
            reply.events_by_task[0].task_info.as_ref().unwrap().name,
            "my_task"
        );
    }

    #[tokio::test]
    async fn test_drop_stats() {
        let mgr = GcsTaskManager::new(1);

        // Add 3 events to force 2 evictions.
        for i in 0..3 {
            mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
                data: Some(TaskEventData {
                    events_by_task: vec![make_task_events(
                        format!("t{i}").as_bytes(),
                        0,
                        b"j1",
                    )],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        }

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.events_by_task.len(), 1);
        assert_eq!(reply.num_status_task_events_dropped, 2); // 2 evicted
    }

    #[tokio::test]
    async fn test_limit_and_truncation_stats() {
        let mgr = GcsTaskManager::new(1000);

        for i in 0..5 {
            mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
                data: Some(TaskEventData {
                    events_by_task: vec![make_task_events(
                        format!("t{i}").as_bytes(),
                        0,
                        b"j1",
                    )],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        }

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest {
                limit: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.events_by_task.len(), 2);
        assert_eq!(reply.num_total_stored, 5);
        assert_eq!(reply.num_truncated, 3);
    }

    #[tokio::test]
    async fn test_job_filter() {
        let mgr = GcsTaskManager::new(1000);

        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![
                    make_task_events(b"t1", 0, b"job_a"),
                    make_task_events(b"t2", 0, b"job_b"),
                ],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        use gcs_proto::ray::rpc::get_task_events_request;
        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest {
                filters: Some(get_task_events_request::Filters {
                    job_filters: vec![get_task_events_request::filters::JobIdFilter {
                        job_id: b"job_a".to_vec(),
                        predicate: 0,
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.events_by_task.len(), 1);
        assert_eq!(reply.events_by_task[0].job_id, b"job_a");
    }

    #[tokio::test]
    async fn test_data_loss_from_worker() {
        let mgr = GcsTaskManager::new(1000);

        // Add a task event.
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![make_task_events(b"t1", 0, b"j1")],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        // Report that this task attempt was dropped by the worker.
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                job_id: b"j1".to_vec(),
                dropped_task_attempts: vec![TaskAttempt {
                    task_id: b"t1".to_vec(),
                    attempt_number: 0,
                }],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        // The dropped task should be removed from storage.
        assert_eq!(reply.events_by_task.len(), 0);
        assert_eq!(reply.num_status_task_events_dropped, 1);
    }

    // ─── Lifecycle listener parity tests ─────────────────────────────

    fn make_running_event(
        task_id: &[u8],
        attempt: i32,
        job_id: &[u8],
        worker_id: Option<Vec<u8>>,
    ) -> TaskEvents {
        let mut e = make_task_events(task_id, attempt, job_id);
        e.task_info = Some(TaskInfoEntry {
            r#type: 0, // NORMAL_TASK — keeps priority bucket out of "finished"
            ..Default::default()
        });
        let mut su = TaskStateUpdate::default();
        su.worker_id = worker_id;
        // Mark it as SUBMITTED_TO_WORKER (5) to simulate a live, non-terminal task.
        su.state_ts_ns
            .insert(TaskStatus::SubmittedToWorker as i32, 1);
        e.state_updates = Some(su);
        e
    }

    /// Parity with C++ `GcsTaskManager::OnWorkerDead` + `MarkTasksFailedOnWorkerDead`.
    #[tokio::test]
    async fn test_on_worker_dead_marks_matching_tasks_failed() {
        let mgr = std::sync::Arc::new(GcsTaskManager::new(1000));

        // Two tasks owned by worker W1, one by worker W2.
        let w1 = vec![1u8; 16];
        let w2 = vec![2u8; 16];
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![
                    make_running_event(b"t1", 0, b"job1", Some(w1.clone())),
                    make_running_event(b"t2", 0, b"job1", Some(w1.clone())),
                    make_running_event(b"t3", 0, b"job1", Some(w2.clone())),
                ],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let worker_data = WorkerTableData {
            exit_type: Some(1),                             // WORKER_DIED
            exit_detail: Some("SIGKILL".into()),
            end_time_ms: 12345,
            ..Default::default()
        };
        mgr.on_worker_dead(w1.clone(), worker_data, 0);
        // on_worker_dead spawns — let it run.
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        // t1 and t2 should now have FAILED state with error info.
        let failed_status = TaskStatus::Failed as i32;
        let mut t1_failed = false;
        let mut t2_failed = false;
        let mut t3_failed = false;
        for e in &reply.events_by_task {
            let has_failed = e
                .state_updates
                .as_ref()
                .map(|su| su.state_ts_ns.contains_key(&failed_status))
                .unwrap_or(false);
            match e.task_id.as_slice() {
                b"t1" => t1_failed = has_failed,
                b"t2" => t2_failed = has_failed,
                b"t3" => t3_failed = has_failed,
                _ => {}
            }
        }
        assert!(t1_failed, "t1 on dead worker should be FAILED");
        assert!(t2_failed, "t2 on dead worker should be FAILED");
        assert!(!t3_failed, "t3 on live worker must remain non-FAILED");

        // Failed event should carry WORKER_DIED error_info.
        let t1 = reply
            .events_by_task
            .iter()
            .find(|e| e.task_id == b"t1")
            .unwrap();
        let err = t1.state_updates.as_ref().unwrap().error_info.as_ref().unwrap();
        assert_eq!(err.error_type, ErrorType::WorkerDied as i32);
        assert!(err.error_message.contains("Worker"));
    }

    /// Already-terminated tasks must not be re-marked (C++ IsTaskTerminated
    /// early-out in MarkTaskAttemptFailedIfNeeded).
    #[tokio::test]
    async fn test_on_worker_dead_skips_terminated_tasks() {
        let mgr = std::sync::Arc::new(GcsTaskManager::new(1000));
        let w1 = vec![1u8; 16];
        // Add a task already marked FINISHED.
        let mut finished = make_running_event(b"done", 0, b"job1", Some(w1.clone()));
        finished
            .state_updates
            .as_mut()
            .unwrap()
            .state_ts_ns
            .insert(TaskStatus::Finished as i32, 99);
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![finished],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        mgr.on_worker_dead(
            w1,
            WorkerTableData {
                end_time_ms: 500,
                ..Default::default()
            },
            0,
        );
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        let su = reply.events_by_task[0].state_updates.as_ref().unwrap();
        // FINISHED must still be set and FAILED must NOT have been added.
        assert!(su.state_ts_ns.contains_key(&(TaskStatus::Finished as i32)));
        assert!(!su.state_ts_ns.contains_key(&(TaskStatus::Failed as i32)));
    }

    /// Parity with C++ `GcsTaskManager::OnJobFinished` + `MarkTasksFailedOnJobEnds`.
    #[tokio::test]
    async fn test_on_job_finished_marks_all_job_tasks_failed() {
        let mgr = std::sync::Arc::new(GcsTaskManager::new(1000));

        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![
                    make_running_event(b"t1", 0, b"jobA", None),
                    make_running_event(b"t2", 0, b"jobA", None),
                    make_running_event(b"t3", 0, b"jobB", None),
                ],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        mgr.on_job_finished(b"jobA".to_vec(), 99999, 0);
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        let failed = TaskStatus::Failed as i32;
        let jobA_failed_count = reply
            .events_by_task
            .iter()
            .filter(|e| {
                e.job_id == b"jobA"
                    && e.state_updates
                        .as_ref()
                        .map(|su| su.state_ts_ns.contains_key(&failed))
                        .unwrap_or(false)
            })
            .count();
        let jobB_failed_count = reply
            .events_by_task
            .iter()
            .filter(|e| {
                e.job_id == b"jobB"
                    && e.state_updates
                        .as_ref()
                        .map(|su| su.state_ts_ns.contains_key(&failed))
                        .unwrap_or(false)
            })
            .count();
        assert_eq!(jobA_failed_count, 2, "both jobA tasks should be FAILED");
        assert_eq!(jobB_failed_count, 0, "jobB tasks must remain non-FAILED");
    }

    /// C++ multiplies end_time_ms by 1_000_000 to produce ns. Rust mirrors.
    #[tokio::test]
    async fn test_on_job_finished_failure_timestamp_is_ns() {
        let mgr = std::sync::Arc::new(GcsTaskManager::new(1000));
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![make_running_event(b"t", 0, b"j", None)],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let finish_ms: i64 = 12_345;
        mgr.on_job_finished(b"j".to_vec(), finish_ms, 0);
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        let ts = *reply.events_by_task[0]
            .state_updates
            .as_ref()
            .unwrap()
            .state_ts_ns
            .get(&(TaskStatus::Failed as i32))
            .expect("FAILED ts_ns must be present");
        assert_eq!(ts, finish_ms * 1_000_000);
    }

    // ─── Blocker-8 parity tests ──────────────────────────────────────
    //
    // Each test references the specific C++ lines the behavior is ported
    // from so future readers can quickly cross-check.

    use gcs_proto::ray::rpc::get_task_events_request::filters::{
        ActorIdFilter, JobIdFilter, StateFilter, TaskIdFilter, TaskNameFilter,
    };
    use gcs_proto::ray::rpc::get_task_events_request::Filters;

    fn filters(f: Filters) -> GetTaskEventsRequest {
        GetTaskEventsRequest {
            filters: Some(f),
            ..Default::default()
        }
    }

    /// 1a — task_filters with a single EQUAL predicate preselects by
    /// task_id, including across attempts. Parity guard for C++
    /// `gcs_task_manager.cc:436-447` and 501-512.
    #[tokio::test]
    async fn task_filter_equal_preselects_by_task_id() {
        let mgr = GcsTaskManager::new(1000);

        let mut want = make_task_events(b"target", 0, b"j1");
        want.task_info.as_mut().unwrap().name = "want".into();
        let mut other = make_task_events(b"other", 0, b"j1");
        other.task_info.as_mut().unwrap().name = "other".into();

        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![want, other],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(filters(Filters {
                task_filters: vec![TaskIdFilter {
                    predicate: FilterPredicate::Equal as i32,
                    task_id: b"target".to_vec(),
                }],
                ..Default::default()
            })))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.events_by_task.len(), 1);
        assert_eq!(reply.events_by_task[0].task_id, b"target");
    }

    /// 1a — task_filters with NOT_EQUAL falls through to the
    /// full-predicate path (no index preselect) and filters correctly.
    #[tokio::test]
    async fn task_filter_not_equal_drops_matching_events() {
        let mgr = GcsTaskManager::new(1000);
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![
                    make_task_events(b"keep", 0, b"j1"),
                    make_task_events(b"drop", 0, b"j1"),
                ],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(filters(Filters {
                task_filters: vec![TaskIdFilter {
                    predicate: FilterPredicate::NotEqual as i32,
                    task_id: b"drop".to_vec(),
                }],
                ..Default::default()
            })))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.events_by_task.len(), 1);
        assert_eq!(reply.events_by_task[0].task_id, b"keep");
    }

    /// 1a — two distinct EQUAL task-id predicates cannot both hold, so
    /// the reply is empty. C++ `gcs_task_manager.cc:443-447`.
    #[tokio::test]
    async fn task_filter_multiple_equal_distinct_returns_empty() {
        let mgr = GcsTaskManager::new(1000);
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![
                    make_task_events(b"a", 0, b"j1"),
                    make_task_events(b"b", 0, b"j1"),
                ],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(filters(Filters {
                task_filters: vec![
                    TaskIdFilter {
                        predicate: FilterPredicate::Equal as i32,
                        task_id: b"a".to_vec(),
                    },
                    TaskIdFilter {
                        predicate: FilterPredicate::Equal as i32,
                        task_id: b"b".to_vec(),
                    },
                ],
                ..Default::default()
            })))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.events_by_task.is_empty());
    }

    /// 1b — events without task_info are skipped unconditionally. C++
    /// `gcs_task_manager.cc:491-495`.
    #[tokio::test]
    async fn events_without_task_info_are_skipped() {
        let mgr = GcsTaskManager::new(1000);
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![
                    make_task_events(b"with_info", 0, b"j1"),
                    make_task_events_no_info(b"no_info", 0, b"j1"),
                ],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        let ids: Vec<_> = reply
            .events_by_task
            .iter()
            .map(|e| e.task_id.clone())
            .collect();
        assert_eq!(ids, vec![b"with_info".to_vec()]);
        // The no_info event was filtered, not absent from storage.
        assert_eq!(reply.num_total_stored, 2);
        assert_eq!(reply.num_filtered_on_gcs, 1);
    }

    /// 1c — malformed predicate surfaces as InvalidArgument. C++
    /// `gcs_task_manager.cc:586-618`.
    #[tokio::test]
    async fn invalid_predicate_returns_invalid_argument() {
        let mgr = GcsTaskManager::new(1000);
        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![make_task_events(b"t", 0, b"j")],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        for (label, f) in [
            (
                "task",
                Filters {
                    task_filters: vec![TaskIdFilter {
                        predicate: 99,
                        task_id: b"t".to_vec(),
                    }],
                    ..Default::default()
                },
            ),
            (
                "job",
                Filters {
                    job_filters: vec![JobIdFilter {
                        predicate: 7,
                        job_id: b"j".to_vec(),
                    }],
                    ..Default::default()
                },
            ),
            (
                "actor",
                Filters {
                    actor_filters: vec![ActorIdFilter {
                        predicate: -1,
                        actor_id: b"a".to_vec(),
                    }],
                    ..Default::default()
                },
            ),
            (
                "task_name",
                Filters {
                    task_name_filters: vec![TaskNameFilter {
                        predicate: 42,
                        task_name: "x".into(),
                    }],
                    ..Default::default()
                },
            ),
            (
                "state",
                Filters {
                    state_filters: vec![StateFilter {
                        predicate: 5,
                        state: "RUNNING".into(),
                    }],
                    ..Default::default()
                },
            ),
        ] {
            let err = mgr
                .get_task_events(Request::new(filters(f)))
                .await
                .err()
                .unwrap_or_else(|| panic!("{label}: expected InvalidArgument"));
            assert_eq!(
                err.code(),
                tonic::Code::InvalidArgument,
                "{label} filter with bogus predicate must be InvalidArgument"
            );
        }
    }

    /// 2a — profile events past `task_events_max_num_profile_events_per_task`
    /// are dropped from the front on merge, and the overflow is counted.
    /// C++ `gcs_task_manager.cc:179-191`.
    #[tokio::test]
    async fn profile_events_truncated_to_per_task_cap() {
        // Pin the per-task cap to 3 independently of global config so
        // the test is hermetic.
        let mgr = GcsTaskManager::new_with_limits(1000, 3);

        let mut ev = make_task_events(b"t", 0, b"j");
        ev.profile_events = Some(gcs_proto::ray::rpc::ProfileEvents {
            component_type: "worker".into(),
            component_id: vec![1],
            node_ip_address: "127.0.0.1".into(),
            events: (0..5)
                .map(|i| gcs_proto::ray::rpc::ProfileEventEntry {
                    event_name: format!("e{i}"),
                    start_time: i as i64,
                    end_time: i as i64 + 1,
                    ..Default::default()
                })
                .collect(),
        });

        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![ev],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.events_by_task.len(), 1);
        let pe = reply.events_by_task[0]
            .profile_events
            .as_ref()
            .expect("profile_events kept");
        assert_eq!(pe.events.len(), 3, "truncated to cap");
        // Oldest two dropped (C++ uses DeleteSubrange(0, to_drop)).
        assert_eq!(pe.events[0].event_name, "e2");
        assert_eq!(pe.events[2].event_name, "e4");
        // The 2 dropped events are reported in the RPC reply.
        assert_eq!(reply.num_profile_task_events_dropped, 2);
    }

    /// 2b — `ray_config::task_events_max_num_task_in_gcs` drives the
    /// storage cap when `GcsServerConfig::max_task_events` is `None`
    /// (the production path). Verified via `new_from_ray_config`.
    #[test]
    fn new_from_ray_config_uses_task_events_max_num_task_in_gcs() {
        // Serialize against the global; other tests may mutate it.
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());
        ray_config::initialize(
            r#"{"task_events_max_num_task_in_gcs": 7, "task_events_max_num_profile_events_per_task": 11}"#,
        )
        .unwrap();

        let mgr = GcsTaskManager::new_from_ray_config();
        assert_eq!(mgr.max_num_task_events, 7);
        assert_eq!(mgr.max_profile_events_per_task, 11);

        let _ = ray_config::replace_for_test(prev);
    }

    /// 1a + 1b together — the full predicate path is exercised end to
    /// end. Adds a mix of events then applies task_filters +
    /// actor_filters + state_filters simultaneously.
    #[tokio::test]
    async fn combined_filters_apply_in_sequence() {
        let mgr = GcsTaskManager::new(1000);

        // Actor task, FINISHED.
        let mut a = make_task_events(b"a", 0, b"j");
        a.task_info.as_mut().unwrap().actor_id = Some(b"actor1".to_vec());
        a.state_updates = Some(TaskStateUpdate {
            state_ts_ns: [(TaskStatus::Finished as i32, 1)].into_iter().collect(),
            ..Default::default()
        });
        // Actor task, still RUNNING.
        let mut b = make_task_events(b"b", 0, b"j");
        b.task_info.as_mut().unwrap().actor_id = Some(b"actor1".to_vec());
        b.state_updates = Some(TaskStateUpdate {
            state_ts_ns: [(TaskStatus::Running as i32, 1)].into_iter().collect(),
            ..Default::default()
        });
        // Non-actor task.
        let c = make_task_events(b"c", 0, b"j");

        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![a, b, c],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        // Ask for actor1 AND state != FINISHED → only `b` matches.
        let reply = mgr
            .get_task_events(Request::new(filters(Filters {
                actor_filters: vec![ActorIdFilter {
                    predicate: FilterPredicate::Equal as i32,
                    actor_id: b"actor1".to_vec(),
                }],
                state_filters: vec![StateFilter {
                    predicate: FilterPredicate::NotEqual as i32,
                    state: "FINISHED".into(),
                }],
                ..Default::default()
            })))
            .await
            .unwrap()
            .into_inner();

        let ids: Vec<_> = reply
            .events_by_task
            .iter()
            .map(|e| e.task_id.clone())
            .collect();
        assert_eq!(ids, vec![b"b".to_vec()]);
    }

    // ─── Blocker-2c parity tests ────────────────────────────────────
    //
    // These mirror the two C++ tests called out in the recheck report:
    // `gcs_task_manager_test.cc:1687-1690` (post-job-finish tracking
    // cleared) and `:1694-1715` (GC caps tracking + preserves total).

    /// 2c.1 — `on_job_finished` must call `update_job_summary_on_job_done`,
    /// which clears the tracked dropped-attempt set. The evicted
    /// counter stays, so `NumTaskAttemptsDropped` still reports the
    /// historical total.
    #[tokio::test]
    async fn on_job_finished_clears_dropped_attempts_tracking() {
        let mgr = std::sync::Arc::new(GcsTaskManager::new(1000));

        // Seed two attempts as dropped for the job (as the
        // eviction / worker-data-loss paths would do).
        {
            let mut storage = mgr.inner.write();
            storage
                .job_summaries
                .entry(b"j".to_vec())
                .or_default()
                .record_task_attempt_dropped(b"t1".to_vec(), 0);
            storage
                .job_summaries
                .entry(b"j".to_vec())
                .or_default()
                .record_task_attempt_dropped(b"t2".to_vec(), 0);
            storage.total_attempts_dropped = 2;
        }

        // Trigger the finalize path (delay 0 keeps the test fast).
        mgr.on_job_finished(b"j".to_vec(), 1_000, 0);
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let storage = mgr.inner.read();
        let summary = storage.job_summaries.get(b"j" as &[u8]).unwrap();
        assert!(
            summary.dropped_task_attempts.is_empty(),
            "on_job_finished must clear dropped_task_attempts tracking"
        );
        // Historical count still reported by the API-facing accessor.
        // C++ parity: `num_dropped_task_attempts_evicted_` stays 0 in
        // this test because we never called GC, so the total is 0 —
        // the point of the test is that tracking is empty, not that
        // the count is preserved here. The preservation story is
        // exercised by the GC test below.
        assert_eq!(summary.num_task_attempts_dropped(), 0);
    }

    /// 2c.2 — `gc_old_dropped_task_attempts(cap)` caps the tracked
    /// set and preserves the total via `num_dropped_task_attempts_evicted`.
    /// Mirrors C++ `TestDroppedTaskAttemptsLimit`
    /// (`gcs_task_manager_test.cc:1694-1715`).
    #[test]
    fn gc_old_dropped_task_attempts_caps_and_preserves_total() {
        let mut summary = JobTaskSummary::default();
        for i in 0..10u8 {
            summary.record_task_attempt_dropped(vec![i], 0);
        }
        assert_eq!(summary.dropped_task_attempts.len(), 10);
        assert_eq!(summary.num_task_attempts_dropped(), 10);

        // Cap at 5 → overflow=5, +10% rounded down = 5 → evict 5.
        // Post-GC: tracked=5, evicted=5, total=10 (matches the C++
        // test's EXPECT_EQ 5 / EXPECT_EQ 5 / EXPECT_EQ 10).
        summary.gc_old_dropped_task_attempts(5);
        assert_eq!(summary.dropped_task_attempts.len(), 5);
        assert_eq!(summary.num_dropped_task_attempts_evicted, 5);
        assert_eq!(summary.num_task_attempts_dropped(), 10);
    }

    /// 2c.3 — the storage-level GC routine walks every job.
    #[test]
    fn storage_gc_job_summary_applies_cap_per_job() {
        let mut storage = TaskEventStorage::new();
        for jobid in [b"ja".to_vec(), b"jb".to_vec()] {
            let s = storage.job_summaries.entry(jobid).or_default();
            for i in 0..10u8 {
                s.record_task_attempt_dropped(vec![i], 0);
            }
        }

        storage.gc_job_summary(3);

        for jobid in [b"ja" as &[u8], b"jb"] {
            let s = storage.job_summaries.get(jobid).unwrap();
            assert!(s.dropped_task_attempts.len() <= 3);
            // tracked + evicted still equals the original 10.
            assert_eq!(s.num_task_attempts_dropped(), 10);
        }
    }

    /// 2c.4 — `get_task_events` on a per-job query reports the
    /// *total* dropped attempts (tracked + evicted), not just the
    /// tracked-set size. Regression guard for the
    /// `summary.dropped_task_attempts.len() as i32` bug the recheck
    /// report called out.
    #[tokio::test]
    async fn get_task_events_reports_post_gc_total_dropped() {
        let mgr = GcsTaskManager::new(1000);

        // Seed and GC to force tracked → evicted movement, then
        // confirm the RPC still reports the original count.
        {
            let mut storage = mgr.inner.write();
            let s = storage.job_summaries.entry(b"j".to_vec()).or_default();
            for i in 0..10u8 {
                s.record_task_attempt_dropped(vec![i], 0);
            }
            storage.total_attempts_dropped = 10;
            storage.gc_job_summary(3);
        }

        let reply = mgr
            .get_task_events(Request::new(filters(Filters {
                job_filters: vec![JobIdFilter {
                    predicate: FilterPredicate::Equal as i32,
                    job_id: b"j".to_vec(),
                }],
                ..Default::default()
            })))
            .await
            .unwrap()
            .into_inner();

        // After GC: tracked set ≤ 3, evicted ≥ 7, sum == 10. Previous
        // Rust code would have reported the tracked-set size (≤ 3)
        // instead of 10.
        assert_eq!(reply.num_status_task_events_dropped, 10);
    }

    /// 2c.5 — `gc_job_summary` on `GcsTaskManager` reads the cap
    /// from `ray_config`, not a hardcoded value. Proves the
    /// `task_events_max_dropped_task_attempts_tracked_per_job_in_gcs`
    /// knob actually reaches the GC routine.
    #[test]
    fn task_manager_gc_job_summary_observes_ray_config() {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = ray_config::replace_for_test(ray_config::RayConfig::default());
        ray_config::initialize(
            r#"{"task_events_max_dropped_task_attempts_tracked_per_job_in_gcs": 4}"#,
        )
        .unwrap();

        let mgr = GcsTaskManager::new(1000);
        {
            let mut storage = mgr.inner.write();
            let s = storage.job_summaries.entry(b"j".to_vec()).or_default();
            for i in 0..10u8 {
                s.record_task_attempt_dropped(vec![i], 0);
            }
        }

        mgr.gc_job_summary();

        let storage = mgr.inner.read();
        let s = storage.job_summaries.get(b"j" as &[u8]).unwrap();
        assert!(s.dropped_task_attempts.len() <= 4);
        assert_eq!(s.num_task_attempts_dropped(), 10);

        let _ = ray_config::replace_for_test(prev);
    }
}
