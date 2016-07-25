#include "ipc.h"

#if defined(__unix__) || defined(__linux__)
#include <sys/statvfs.h>
#endif

#include <stdlib.h>
#include "ray/ray.h"

ObjHandle::ObjHandle(SegmentId segmentid, size_t size, IpcPointer ipcpointer, size_t metadata_offset)
  : segmentid_(segmentid), size_(size), ipcpointer_(ipcpointer), metadata_offset_(metadata_offset)
{}

MessageQueue<>::MessageQueue() : create_(false) { }

MessageQueue<>::~MessageQueue() {
  if (!name_.empty() && create_) {
    // Only remove the message queue if we created it.
    RAY_LOG(RAY_DEBUG, "Removing message queue " << name_.c_str() << ", create = " << create_);
    bip::message_queue::remove(name_.c_str());
  }
}

MessageQueue<>::MessageQueue(MessageQueue&& other) {
  *this = std::move(other);
}

MessageQueue<>& MessageQueue<>::operator=(MessageQueue&& other) {
  name_ = std::move(other.name_);
  create_ = other.create_;
  queue_ = std::move(other.queue_);
  other.name_.clear();  // It is unclear if this is guaranteed, but we need it to hold for the destructor. See: https://stackoverflow.com/a/17735913

  return *this;
}


bool MessageQueue<>::connect(const std::string& name, bool create, size_t message_size, size_t message_capacity) {
  name_ = name;
  name_.insert(0, "ray-{BC200A09-2465-431D-AEC7-2F8530B04535}-");
#if defined(WIN32) || defined(_WIN32)
  std::replace(name_.begin(), name_.end(), ':', '-');
#endif
  try {
    if (create) {
      bip::message_queue::remove(name_.c_str()); // remove queue if it has not been properly removed from last run
      queue_ = std::unique_ptr<bip::message_queue>(new bip::message_queue(bip::create_only, name_.c_str(), message_capacity, message_size));
      create_ = true; // Only set create_ = true on success.
    }
    else {
      queue_ = std::unique_ptr<bip::message_queue>(new bip::message_queue(bip::open_only, name_.c_str()));
    }
  }
  catch (bip::interprocess_exception &ex) {
    RAY_CHECK(false, "boost::interprocess exception: " << ex.what());
  }
  return true;
}
bool MessageQueue<>::connected() {
  return queue_ != NULL;
}

bool MessageQueue<>::send(const void * object, size_t size) {
  try {
    queue_->send(object, size, 0);
  }
  catch (bip::interprocess_exception &ex) {
    RAY_CHECK(false, "boost::interprocess exception: " << ex.what());
  }
  return true;
}

bool MessageQueue<>::receive(void * object, size_t size) {
  unsigned int priority;
  bip::message_queue::size_type recvd_size;
  try {
    queue_->receive(object, size, recvd_size, priority);
  }
  catch (bip::interprocess_exception &ex) {
    RAY_CHECK(false, "boost::interprocess exception: " << ex.what());
  }
  return true;
}

MemorySegmentPool::MemorySegmentPool(ObjStoreId objstoreid, bool create) : objstoreid_(objstoreid), create_mode_(create) { }

// creates a memory segment if it is not already there; if the pool is in create mode,
// space is allocated, if it is in open mode, the shared memory is mapped into the process
void MemorySegmentPool::open_segment(SegmentId segmentid, size_t size) {
  RAY_LOG(RAY_DEBUG, "Opening segmentid " << segmentid << " on object store " << objstoreid_ << " with create_mode_ = " << create_mode_);
  RAY_CHECK(segmentid == segments_.size() || !create_mode_, "Object store " << objstoreid_ << " is attempting to open segmentid " << segmentid << " on the object store, but segments_.size() = " << segments_.size());
  if (segmentid >= segments_.size()) { // resize and initialize segments_
    int current_size = segments_.size();
    segments_.resize(segmentid + 1);
    for (int i = current_size; i < segments_.size(); ++i) {
      segments_[i].first = nullptr;
      segments_[i].second = SegmentStatusType::UNOPENED;
    }
  }
  if (segments_[segmentid].second == SegmentStatusType::OPENED) {
    return;
  }
  RAY_CHECK_NEQ(segments_[segmentid].second, SegmentStatusType::CLOSED, "Attempting to open segmentid " << segmentid << ", but segments_[segmentid].second == SegmentStatusType::CLOSED.");
  std::string segment_name = get_segment_name(segmentid);
  if (create_mode_) {
    assert(size > 0);
    bip::shared_memory_object::remove(segment_name.c_str()); // remove segment if it has not been properly removed from last run
    size_t new_size = (size / page_size_ + 2) * page_size_; // additional room for boost's bookkeeping
    segments_[segmentid] = std::make_pair(std::unique_ptr<bip::managed_shared_memory>(new bip::managed_shared_memory(bip::create_only, segment_name.c_str(), new_size)), SegmentStatusType::OPENED);
  } else {
    segments_[segmentid] = std::make_pair(std::unique_ptr<bip::managed_shared_memory>(new bip::managed_shared_memory(bip::open_only, segment_name.c_str())), SegmentStatusType::OPENED);
  }
}

void MemorySegmentPool::unmap_segment(SegmentId segmentid) {
  segments_[segmentid].first.reset();
  segments_[segmentid].second = SegmentStatusType::UNOPENED;
}

void MemorySegmentPool::close_segment(SegmentId segmentid) {
  RAY_LOG(RAY_DEBUG, "closing segmentid " << segmentid);
  std::string segment_name = get_segment_name(segmentid);
  bip::shared_memory_object::remove(segment_name.c_str());
  segments_[segmentid].first.reset();
  segments_[segmentid].second = SegmentStatusType::CLOSED;
}

ObjHandle MemorySegmentPool::allocate(size_t size) {
  RAY_CHECK(create_mode_, "Attempting to call allocate, but create_mode_ is false");
  // TODO(pcm): at the moment, this always creates a new segment, this will be changed
  SegmentId segmentid = segments_.size();
  open_segment(segmentid, size);
  objstore_memcheck(size);
  void* ptr = segments_[segmentid].first->allocate(size);
  auto handle = segments_[segmentid].first->get_handle_from_address(ptr);
  return ObjHandle(segmentid, size, handle);
}

void MemorySegmentPool::deallocate(ObjHandle pointer) {
  SegmentId segmentid = pointer.segmentid();
  void* ptr = segments_[segmentid].first->get_address_from_handle(pointer.ipcpointer());
  segments_[segmentid].first->deallocate(ptr);
  close_segment(segmentid);
}

// returns address of the object refered to by the handle, needs to be called on
// the process that will use the address
uint8_t* MemorySegmentPool::get_address(ObjHandle pointer) {
  RAY_CHECK(!create_mode_ || segments_[pointer.segmentid()].second == SegmentStatusType::OPENED, "Object store " << objstoreid_ << " is attempting to call get_address on segmentid " << pointer.segmentid() << ", which has not been opened yet.");
  if (!create_mode_) {
    open_segment(pointer.segmentid());
  }
  bip::managed_shared_memory* segment = segments_[pointer.segmentid()].first.get();
  return static_cast<uint8_t*>(segment->get_address_from_handle(pointer.ipcpointer()));
}

// returns the name of the segment
std::string MemorySegmentPool::get_segment_name(SegmentId segmentid) {
  return std::string("ray-{BC200A09-2465-431D-AEC7-2F8530B04535}-objstore-") + std::to_string(objstoreid_) + std::string("-segment-") + std::to_string(segmentid);
}

MemorySegmentPool::~MemorySegmentPool() {
  destroy_segments();
}

void MemorySegmentPool::objstore_memcheck(int64_t size) {
#if defined(__unix__) || defined(__linux__)
  struct statvfs buffer;
  statvfs("/dev/shm/", &buffer);
  if (size + 100 > buffer.f_bsize * buffer.f_bavail) {
    MemorySegmentPool::destroy_segments();
    RAY_LOG(RAY_FATAL, "Not enough memory for allocating object in objectstore.");
  }
#endif
}

void MemorySegmentPool::destroy_segments() {
  for (size_t segmentid = 0; segmentid < segments_.size(); ++segmentid) {
    std::string segment_name = get_segment_name(segmentid);
    segments_[segmentid].first.reset();
    bip::shared_memory_object::remove(segment_name.c_str());
  }
}
#if defined(WIN32) || defined(_WIN32)
namespace boost {
  namespace interprocess {
    namespace ipcdetail {
      windows_bootstamp windows_intermodule_singleton<windows_bootstamp>::get() {
        // HACK: Only do this for Windows as there seems to be no better workaround. Possibly undefined behavior!
        return reinterpret_cast<windows_bootstamp const &>(std::string("BOOTSTAMP"));
      }
    }
  }
}
#endif
