// Copyright 2020-2021 The Ray Authors.
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

#include "ray/object_manager/common.h"

#include "absl/functional/bind_front.h"
#include "absl/strings/str_format.h"

namespace ray {

void PlasmaObjectHeader::Init() {
#if defined(__APPLE__) || defined(__linux__)
  memset(unique_name, 0, sizeof(unique_name));

  semaphores_created = SemaphoresCreationLevel::kUnitialized;

  pid_t pid = getpid();
  std::string name =
      absl::StrCat(pid, "-", absl::ToInt64Nanoseconds(absl::Now() - absl::UnixEpoch()));
  RAY_CHECK_LE(name.size(), PSEMNAMLEN);
  memcpy(unique_name, name.c_str(), name.size());
#endif  // defined(__APPLE__) || defined(__linux__)

  version = 0;
  is_sealed = false;
  has_error = false;
  num_readers = 0;
  num_read_acquires_remaining = 0;
  num_read_releases_remaining = 0;
  data_size = 0;
  metadata_size = 0;
}

void PrintPlasmaObjectHeader(const PlasmaObjectHeader *header) {
  std::string print;
  absl::StrAppend(&print, "PlasmaObjectHeader: \n");
#if defined(__APPLE__) || defined(__linux__)
  absl::StrAppend(&print,
                  "semaphores_created: ",
                  header->semaphores_created.load(std::memory_order_relaxed),
                  "\n");
  absl::StrAppend(&print, "unique_name: ", header->unique_name, "\n");
#endif  // defined(__APPLE__) || defined(__linux__)
  absl::StrAppend(&print, "version: ", header->version, "\n");
  absl::StrAppend(&print, "num_readers: ", header->num_readers, "\n");
  absl::StrAppend(
      &print, "num_read_acquires_remaining: ", header->num_read_acquires_remaining, "\n");
  absl::StrAppend(
      &print, "num_read_releases_remaining: ", header->num_read_releases_remaining, "\n");
  absl::StrAppend(&print, "data_size: ", header->data_size, "\n");
  absl::StrAppend(&print, "metadata_size: ", header->metadata_size, "\n");
  RAY_LOG(DEBUG) << print;
}

Status PlasmaObjectHeader::CheckHasError() const {
  // We do an acquire load so that no loads/stores are reordered before the load to
  // `has_error`. This acquire load pairs with the release store in `SetErrorUnlocked()`.
  if (has_error.load(std::memory_order_acquire)) {
    return Status::ChannelError("Channel closed.");
  }
  return Status::OK();
}

#if defined(__APPLE__) || defined(__linux__)

Status PlasmaObjectHeader::TryToAcquireSemaphore(
    sem_t *sem,
    const std::unique_ptr<std::chrono::steady_clock::time_point> &timeout_point) const {
  // Check `has_error` first to avoid blocking forever on the semaphore.
  RAY_RETURN_NOT_OK(CheckHasError());

  if (!timeout_point) {
    RAY_CHECK_EQ(sem_wait(sem), 0);
  } else {
    bool got_sem = false;
    // try to acquire the semaphore at least once even if the timeout_point is passed
    do {
      // macOS does not support sem_timedwait, so we implement a unified,
      // spinning-based solution here
      if (sem_trywait(sem) == 0) {
        got_sem = true;
        break;
      }
    } while (std::chrono::steady_clock::now() < *timeout_point);
    if (!got_sem) {
      return Status::ChannelTimeoutError("Timed out waiting for semaphore.");
    }
  }

  // Check `has_error` again so that no more than one thread is ever in the critical
  // section after `SetErrorUnlocked()` has been called. One thread could be in the
  // critical section when that is called, but no additional thread will enter the
  // critical section.
  Status s = CheckHasError();
  if (!s.ok()) {
    RAY_CHECK_EQ(sem_post(sem), 0);
  }
  return s;
}

void PlasmaObjectHeader::SetErrorUnlocked(Semaphores &sem) {
  RAY_CHECK(sem.header_sem);
  RAY_CHECK(sem.object_sem);

  // We do a store release so that no loads/stores are reordered after the store to
  // `has_error`. This store release pairs with the acquire load in `CheckHasError()`.
  has_error.store(true, std::memory_order_release);
  // Increment `sem.object_sem` once to potentially unblock the writer. There will never
  // be more than one writer.
  RAY_CHECK_EQ(sem_post(sem.object_sem), 0);

  // Increment `header_sem` to unblock any readers and/or the writer.
  RAY_CHECK_EQ(sem_post(sem.header_sem), 0);
}

Status PlasmaObjectHeader::WriteAcquire(
    Semaphores &sem,
    uint64_t write_data_size,
    uint64_t write_metadata_size,
    int64_t write_num_readers,
    const std::unique_ptr<std::chrono::steady_clock::time_point> &timeout_point) {
  RAY_CHECK(sem.object_sem);
  RAY_CHECK(sem.header_sem);

  RAY_RETURN_NOT_OK(TryToAcquireSemaphore(sem.object_sem, timeout_point));
  // Header is locked only for a short time, so we don't have to apply the
  // same `timeout_point`.
  RAY_RETURN_NOT_OK(TryToAcquireSemaphore(sem.header_sem));

  RAY_CHECK_EQ(num_read_acquires_remaining, 0UL);
  RAY_CHECK_EQ(num_read_releases_remaining, 0UL);

  version++;
  is_sealed = false;
  data_size = write_data_size;
  metadata_size = write_metadata_size;
  num_readers = write_num_readers;

  RAY_CHECK_EQ(sem_post(sem.header_sem), 0);
  return Status::OK();
}

Status PlasmaObjectHeader::WriteRelease(Semaphores &sem) {
  // Header is locked only for a short time, so we don't have to apply the
  // same `timeout_point`.
  RAY_RETURN_NOT_OK(TryToAcquireSemaphore(sem.header_sem));

  is_sealed = true;
  RAY_CHECK(num_readers) << num_readers;
  num_read_acquires_remaining = num_readers;
  num_read_releases_remaining = num_readers;

  RAY_CHECK_EQ(sem_post(sem.header_sem), 0);
  return Status::OK();
}

Status PlasmaObjectHeader::ReadAcquire(
    const ObjectID &object_id,
    Semaphores &sem,
    int64_t version_to_read,
    int64_t &version_read,
    const std::unique_ptr<std::chrono::steady_clock::time_point> &timeout_point) {
  RAY_CHECK(sem.header_sem);

  // Header is locked only for a short time, so we don't have to apply the
  // same `timeout_point`.
  RAY_RETURN_NOT_OK(TryToAcquireSemaphore(sem.header_sem));

  // TODO(jhumphri): Wouldn't a futex be better here than polling?
  // Wait for the requested version (or a more recent one) to be sealed.
  while (version < version_to_read || !is_sealed) {
    RAY_CHECK_EQ(sem_post(sem.header_sem), 0);
    sched_yield();
    // We need to get the desired version before timeout
    if (timeout_point && std::chrono::steady_clock::now() >= *timeout_point) {
      return Status::ChannelTimeoutError(absl::StrCat(
          "Timed out waiting for object available to read. ObjectID: ", object_id.Hex()));
    }
    // Unlike other header, this is used for busy waiting, so we need to apply
    // timeout_point.
    RAY_RETURN_NOT_OK(TryToAcquireSemaphore(sem.header_sem, timeout_point));
  }

  bool success = false;
  if (num_readers == -1) {
    // Object is a normal immutable object. Read succeeds.
    version_read = 0;
    success = true;
  } else {
    version_read = version;
    if (version == version_to_read && num_read_acquires_remaining > 0) {
      // This object is at the right version and still has reads remaining. Read
      // succeeds.
      num_read_acquires_remaining--;
      success = true;
    }
  }

  RAY_CHECK_EQ(sem_post(sem.header_sem), 0);
  if (!success) {
    return Status::Invalid(
        "Reader missed a value. Are you sure there are num_readers many readers?");
  }
  return Status::OK();
}

Status PlasmaObjectHeader::ReadRelease(Semaphores &sem, int64_t read_version) {
  RAY_CHECK(sem.object_sem);
  RAY_CHECK(sem.header_sem);

  bool all_readers_done = false;
  RAY_RETURN_NOT_OK(TryToAcquireSemaphore(sem.header_sem));

  RAY_CHECK_EQ(version, read_version)
      << "Version " << version << " modified from version " << read_version
      << " at read start";

  if (num_readers != -1) {
    RAY_CHECK_GT(num_read_releases_remaining, 0UL);
    num_read_releases_remaining--;
    RAY_CHECK_GE(num_read_releases_remaining, 0UL);
    all_readers_done = !num_read_releases_remaining;
  }

  RAY_CHECK_EQ(sem_post(sem.header_sem), 0);
  if (all_readers_done) {
    RAY_CHECK_EQ(sem_post(sem.object_sem), 0);
  }
  return Status::OK();
}

#else  // defined(__APPLE__) || defined(__linux__)

Status PlasmaObjectHeader::TryToAcquireSemaphore(
    sem_t *sem,
    const std::unique_ptr<std::chrono::steady_clock::time_point> &timeout_point) const {
  return Status::NotImplemented("Not supported on Windows.");
}

void PlasmaObjectHeader::SetErrorUnlocked(Semaphores &sem) {}

Status PlasmaObjectHeader::WriteAcquire(
    Semaphores &sem,
    uint64_t write_data_size,
    uint64_t write_metadata_size,
    int64_t write_num_readers,
    const std::unique_ptr<std::chrono::steady_clock::time_point> &timeout_point) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status PlasmaObjectHeader::WriteRelease(Semaphores &sem) {
  return Status::NotImplemented("Not supported on Windows.");
}

Status PlasmaObjectHeader::ReadAcquire(
    const ObjectID &object_id,
    Semaphores &sem,
    int64_t version_to_read,
    int64_t &version_read,
    const std::unique_ptr<std::chrono::steady_clock::time_point> &timeout_point) {
  return Status::NotImplemented("Not supported on Windows.");
}

#endif

}  // namespace ray
