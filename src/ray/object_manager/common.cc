#include "ray/object_manager/common.h"


namespace ray {

void PlasmaObjectHeader::Init() {
  // mut is shared between writer and readers.
	pthread_mutexattr_t mutex_attr;
	pthread_mutexattr_init(&mutex_attr);
	pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
  pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_ERRORCHECK);
	pthread_mutex_init(&mut, &mutex_attr);

  // Condition is shared between writer and readers.
  pthread_condattr_t cond_attr;
  pthread_condattr_init(&cond_attr);
  pthread_condattr_setpshared(&cond_attr, PTHREAD_PROCESS_SHARED);
  pthread_cond_init(&cond, &cond_attr);
}

void PlasmaObjectHeader::Destroy() {
  RAY_CHECK(pthread_mutex_destroy(&mut) == 0);
  RAY_CHECK(pthread_cond_destroy(&cond) == 0);
}

void PlasmaObjectHeader::WriteAcquire(int64_t write_version) {
  RAY_CHECK(pthread_mutex_lock(&mut) == 0);

  while (num_reads_remaining > 0) {
    RAY_CHECK(pthread_cond_wait(&cond, &mut) == 0);
  }

  num_readers_acquired = 0;
  RAY_LOG(DEBUG) << "WriteAcquire " << write_version;
  RAY_CHECK(write_version > version) << version;
  if (write_version != version + 1) {
    RAY_LOG(WARNING) << "Write version " << write_version
      << " is more than 1 greater than current version " << version
      << ". Are you sure this is the only writer?";
  }
  version = write_version;
  RAY_CHECK(pthread_mutex_unlock(&mut) == 0);
}

void PlasmaObjectHeader::WriteRelease(int64_t write_version, int64_t write_max_readers) {
  RAY_CHECK(pthread_mutex_lock(&mut) == 0);

  RAY_CHECK(version == write_version);
  RAY_LOG(DEBUG) << "WriteRelease " << write_version;
  max_readers = write_max_readers;
  num_reads_remaining = max_readers;

  RAY_CHECK(pthread_mutex_unlock(&mut) == 0);
  // Signal to all readers.
  RAY_CHECK(pthread_cond_broadcast(&cond) == 0);
}

int64_t PlasmaObjectHeader::ReadAcquire(int64_t read_version) {
  RAY_CHECK(pthread_mutex_lock(&mut) == 0);
  RAY_LOG(DEBUG) << "ReadAcquire " << read_version;

  while (version < read_version) {
    RAY_LOG(DEBUG) << "ReadAcquire " << read_version << ", version is currently " << version;
    RAY_CHECK(pthread_cond_wait(&cond, &mut) == 0);
  }

  if (version > read_version) {
    RAY_LOG(WARNING) << "Version " << version << " already exceeds version to read " << read_version;
  }

  // TODO(swang): Plasma store currently uses ReadAcquire to
  // wait for the object to become sealed.
  // num_reads_remaining can be 0 because the plasma stores
  // reads concurrently with other readers.
  //RAY_CHECK(num_reads_remaining >= 0);
  num_readers_acquired++;
  if (max_readers != -1 && num_readers_acquired > max_readers) {
    RAY_LOG(WARNING) << num_readers_acquired << " readers acquired exceeds max readers " << max_readers;
  }

  read_version = version;
  RAY_CHECK(pthread_mutex_unlock(&mut) == 0);
  return read_version;
}

void PlasmaObjectHeader::ReadRelease(int64_t read_version) {
  bool all_readers_done = false;
  RAY_CHECK(pthread_mutex_lock(&mut) == 0);

  RAY_LOG(DEBUG) << "ReadRelease " << read_version << " version is currently " << version;
  RAY_CHECK(version == read_version) << "Version " << version << " modified from version " << read_version << " at read start";

  if (num_reads_remaining != -1) {
    num_reads_remaining--;
    RAY_CHECK(num_reads_remaining >= 0);
    if (num_reads_remaining == 0) {
      all_readers_done = true;
    }
  }

  RAY_CHECK(pthread_mutex_unlock(&mut) == 0);
  if (all_readers_done) {
    RAY_CHECK(pthread_cond_signal(&cond) == 0);
  }
}

}  // namespace ray
