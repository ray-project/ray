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
  if (segmentid < segments_.size()) {
    return;
  }
  segment_names_.resize(segmentid + 1);
  segments_.resize(segmentid + 1);
  std::string segment_name = std::string("segment:") + std::to_string(segmentid);
  if (create_mode_) {
    assert(size > 0);
    shared_memory_object::remove(segment_name.c_str()); // remove segment if it has not been properly removed from last run
    size_t new_size = (size / page_size_ + 2) * page_size_; // additional room for boost's bookkeeping
    segments_[segmentid] = std::unique_ptr<managed_shared_memory>(new managed_shared_memory(create_only, segment_name.c_str(), new_size));
  } else {
    segments_[segmentid] = std::unique_ptr<managed_shared_memory>(new managed_shared_memory(open_only, segment_name.c_str()));
  }
  segment_names_[segmentid] = segment_name;
}

ObjHandle MemorySegmentPool::allocate(size_t size) {
  // TODO(pcm): at the moment, this always creates a new segment, this will be changed
  SegmentId segmentid = segment_names_.size();
  open_segment(segmentid, size);
  void* ptr = segments_[segmentid]->allocate(size);
  auto handle = segments_[segmentid]->get_handle_from_address(ptr);
  return ObjHandle(segmentid, size, handle);
}

// returns address of the object refered to by the handle, needs to be called on
// the process that will use the address
uint8_t* MemorySegmentPool::get_address(ObjHandle pointer) {
  if (pointer.segmentid() >= segments_.size()) {
    for (int i = segments_.size(); i <= pointer.segmentid(); ++i) {
      open_segment(i);
    }
  }
  managed_shared_memory* segment = segments_[pointer.segmentid()].get();
  return static_cast<uint8_t*>(segment->get_address_from_handle(pointer.ipcpointer()));
}

MemorySegmentPool::~MemorySegmentPool() {
  assert(segment_names_.size() == segments_.size());
  for (size_t i = 0; i < segment_names_.size(); ++i) {
    segments_[i].reset();
    shared_memory_object::remove(segment_names_[i].c_str());
  }
}
