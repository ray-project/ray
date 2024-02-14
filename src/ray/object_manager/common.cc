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

namespace ray {

void PlasmaObjectHeader::Init() {
#ifdef __linux__
  // wr_mut is shared between writer and readers.
  pthread_mutexattr_t mutex_attr;
  pthread_mutexattr_init(&mutex_attr);
  pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
  pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_ERRORCHECK);
  pthread_mutex_init(&wr_mut, &mutex_attr);

  sem_init(&rw_semaphore, PTHREAD_PROCESS_SHARED, 1);

  version = 0;
  is_sealed = false;
  has_error = false;
  num_readers = 0;
  num_read_acquires_remaining = 0;
  num_read_releases_remaining = 0;
  data_size = 0;
  metadata_size = 0;
#endif
}

void PlasmaObjectHeader::Destroy() {
#ifdef __linux__
  RAY_CHECK(pthread_mutex_destroy(&wr_mut) == 0);
  RAY_CHECK(sem_destroy(&rw_semaphore) == 0);
#endif
}

#ifdef __linux__

void PrintPlasmaObjectHeader(const PlasmaObjectHeader *header) {
  RAY_LOG(DEBUG) << "PlasmaObjectHeader: \n"
                 << "version: " << header->version << "\n"
                 << "num_readers: " << header->num_readers << "\n"
                 << "num_read_acquires_remaining: " << header->num_read_acquires_remaining
                 << "\n"
                 << "num_read_releases_remaining: " << header->num_read_releases_remaining
                 << "\n"
                 << "data_size: " << header->data_size << "\n"
                 << "metadata_size: " << header->metadata_size << "\n";
}

Status PlasmaObjectHeader::TryAcquireWriterMutex() {
  // Try to acquire the lock, checking every 1s for the error bit.
  struct timespec ts;
  do {
    if (has_error) {
      return Status::IOError("channel closed");
    }
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1;
  } while (pthread_mutex_timedlock(&wr_mut, &ts));

  return Status::OK();
}

Status PlasmaObjectHeader::WriteAcquire(uint64_t write_data_size,
                                        uint64_t write_metadata_size,
                                        int64_t write_num_readers) {
  RAY_LOG(DEBUG) << "WriteAcquire. data size " << write_data_size << ", metadata size "
                 << write_metadata_size << ", num readers: " << write_num_readers;

  // Try to acquire the semaphore, checking every 1s for the error bit.
  struct timespec ts;
  do {
    if (has_error) {
      return Status::IOError("channel closed");
    }
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1;
  } while (sem_timedwait(&rw_semaphore, &ts));

  RAY_RETURN_NOT_OK(TryAcquireWriterMutex());
  PrintPlasmaObjectHeader(this);

  RAY_CHECK(num_read_acquires_remaining == 0);
  RAY_CHECK(num_read_releases_remaining == 0);

  version++;
  is_sealed = false;
  data_size = write_data_size;
  metadata_size = write_metadata_size;
  num_readers = write_num_readers;

  RAY_LOG(DEBUG) << "WriteAcquire done";
  PrintPlasmaObjectHeader(this);
  RAY_CHECK(pthread_mutex_unlock(&wr_mut) == 0);
  return Status::OK();
}

Status PlasmaObjectHeader::WriteRelease() {
  RAY_LOG(DEBUG) << "WriteRelease Waiting. version: " << version;
  RAY_RETURN_NOT_OK(TryAcquireWriterMutex());
  RAY_LOG(DEBUG) << "WriteRelease " << version;
  PrintPlasmaObjectHeader(this);

  is_sealed = true;
  RAY_CHECK(num_readers != 0) << num_readers;
  num_read_acquires_remaining = num_readers;
  num_read_releases_remaining = num_readers;

  RAY_LOG(DEBUG) << "WriteRelease done, num_readers: " << num_readers;
  PrintPlasmaObjectHeader(this);
  RAY_CHECK(pthread_mutex_unlock(&wr_mut) == 0);
  return Status::OK();
}

Status PlasmaObjectHeader::ReadAcquire(int64_t version_to_read, int64_t *version_read) {
  RAY_LOG(DEBUG) << "ReadAcquire waiting version " << version_to_read;
  RAY_RETURN_NOT_OK(TryAcquireWriterMutex());
  RAY_LOG(DEBUG) << "ReadAcquire " << version_to_read;
  PrintPlasmaObjectHeader(this);

  // Wait for the requested version (or a more recent one) to be sealed.
  int tries = 0;
  while (version < version_to_read || !is_sealed) {
    RAY_CHECK(pthread_mutex_unlock(&wr_mut) == 0);
    // Lower values than 100k seem to start impacting perf compared
    // to mutex.
    if (tries++ > 100000) {
      std::this_thread::yield();  // Too many tries, yield thread.
    }
    RAY_RETURN_NOT_OK(TryAcquireWriterMutex());
  }

  bool success = false;
  if (num_readers == -1) {
    // Object is a normal immutable object. Read succeeds.
    *version_read = 0;
    success = true;
  } else {
    *version_read = version;
    if (version == version_to_read && num_read_acquires_remaining > 0) {
      // This object is at the right version and still has reads remaining. Read
      // succeeds.
      num_read_acquires_remaining--;
      success = true;
    } else if (version > version_to_read) {
      RAY_LOG(WARNING) << "Version " << version << " already exceeds version to read "
                       << version_to_read;
    } else {
      RAY_LOG(WARNING) << "Version " << version << " already has " << num_readers
                       << " readers";
    }
  }

  RAY_LOG(DEBUG) << "ReadAcquire done";
  PrintPlasmaObjectHeader(this);

  RAY_CHECK(pthread_mutex_unlock(&wr_mut) == 0);
  if (!success) {
    return Status::Invalid(
        "Reader missed a value. Are you sure there are num_readers many readers?");
  }
  return Status::OK();
}

Status PlasmaObjectHeader::ReadRelease(int64_t read_version) {
  bool all_readers_done = false;
  RAY_LOG(DEBUG) << "ReadRelease Waiting" << read_version;
  RAY_RETURN_NOT_OK(TryAcquireWriterMutex());
  PrintPlasmaObjectHeader(this);

  RAY_LOG(DEBUG) << "ReadRelease " << read_version << " version is currently " << version;
  RAY_CHECK(version == read_version) << "Version " << version << " modified from version "
                                     << read_version << " at read start";

  if (num_readers != -1) {
    num_read_releases_remaining--;
    RAY_CHECK(num_read_releases_remaining >= 0);
    if (num_read_releases_remaining == 0) {
      all_readers_done = true;
    }
  }

  PrintPlasmaObjectHeader(this);
  RAY_LOG(DEBUG) << "ReadRelease done";
  RAY_CHECK(pthread_mutex_unlock(&wr_mut) == 0);
  if (all_readers_done) {
    sem_post(&rw_semaphore);
  }
  return Status::OK();
}

#endif

}  // namespace ray
