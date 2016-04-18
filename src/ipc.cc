#include "ipc.h"

using namespace arrow;

ObjHandle::ObjHandle(SegmentId segmentid, size_t size, IpcPointer ipcpointer, size_t metadata_offset)
  : segmentid_(segmentid), size_(size), ipcpointer_(ipcpointer), metadata_offset_(metadata_offset)
{}

Status BufferMemorySource::Write(int64_t position, const uint8_t* data, int64_t nbytes) {
  // TODO(pcm): error handling
  std::memcpy(data_ + position, data, nbytes);
  return Status::OK();
}

Status BufferMemorySource::ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
  // TODO(pcm): error handling
  *out = std::make_shared<Buffer>(data_ + position, nbytes);
  return Status::OK();
}

Status BufferMemorySource::Close() {
  return Status::OK();
}

int64_t BufferMemorySource::Size() const {
  return size_;
}

MemorySegmentPool::MemorySegmentPool(bool create) : create_mode_(create) { }

// creates a memory segment if it is not already there; if the pool is in create mode,
// space is allocated, if it is in open mode, the shared memory is mapped into the process
void MemorySegmentPool::open_segment(SegmentId segmentid, size_t size) {
  ORCH_LOG(ORCH_DEBUG, "OPENING segmentid " << segmentid);
  if (segmentid != segments_.size() && create_mode_) {
    ORCH_LOG(ORCH_FATAL, "Attempting to open segmentid " << segmentid << " on the object store, but segments_.size() = " << segments_.size());
  }
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
  if (segments_[segmentid].second == SegmentStatusType::CLOSED) {
    ORCH_LOG(ORCH_FATAL, "Attempting to open segmentid " << segmentid << ", but segments_[segmentid].second == SegmentStatusType::CLOSED.");
  }
  std::string segment_name = std::string("segment:") + std::to_string(segmentid);
  if (create_mode_) {
    assert(size > 0);
    shared_memory_object::remove(segment_name.c_str()); // remove segment if it has not been properly removed from last run
    size_t new_size = (size / page_size_ + 2) * page_size_; // additional room for boost's bookkeeping
    segments_[segmentid] = std::make_pair(std::unique_ptr<managed_shared_memory>(new managed_shared_memory(create_only, segment_name.c_str(), new_size)), SegmentStatusType::OPENED);
  } else {
    segments_[segmentid] = std::make_pair(std::unique_ptr<managed_shared_memory>(new managed_shared_memory(open_only, segment_name.c_str())), SegmentStatusType::OPENED);
  }
}

void MemorySegmentPool::close_segment(SegmentId segmentid) {
  ORCH_LOG(ORCH_DEBUG, "CLOSING segmentid " << segmentid);
  std::string segment_name = std::string("segment:") + std::to_string(segmentid);
  shared_memory_object::remove(segment_name.c_str());
  segments_[segmentid].first.reset();
  segments_[segmentid].second = SegmentStatusType::CLOSED;
}

ObjHandle MemorySegmentPool::allocate(size_t size) {
  if (!create_mode_) { // allocate is called only by the object store
    ORCH_LOG(ORCH_FATAL, "Attempting to call allocate, but create_mode_ is false");
  }
  // TODO(pcm): at the moment, this always creates a new segment, this will be changed
  SegmentId segmentid = segments_.size();
  open_segment(segmentid, size);
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
  open_segment(pointer.segmentid());
  managed_shared_memory* segment = segments_[pointer.segmentid()].first.get();
  return static_cast<uint8_t*>(segment->get_address_from_handle(pointer.ipcpointer()));
}

MemorySegmentPool::~MemorySegmentPool() {
  for (size_t segmentid = 0; segmentid < segments_.size(); ++segmentid) {
    std::string segment_name = std::string("segment:") + std::to_string(segmentid);
    segments_[segmentid].first.reset();
    shared_memory_object::remove(segment_name.c_str());
  }
}
