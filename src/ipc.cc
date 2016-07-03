#include "ipc.h"

#include <stdlib.h>

#if defined(WIN32) || defined(_WIN32)
#include <Windows.h>
#else
#include <sys/types.h>
#include <sys/stat.h>
#endif

#include "ray/ray.h"

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

MessageQueue<>::MessageQueue() : handle_(-1) { }

MessageQueue<>::~MessageQueue() { close(); }

MessageQueue<>::MessageQueue(MessageQueue&& other) {
  handle_ = other.handle_;
  other.handle_ = -1;
}

MessageQueue<>& MessageQueue<>::operator=(MessageQueue<>&& other) {
  close();
  handle_ = other.handle_;
  other.handle_ = -1;
  return *this;
}

bool MessageQueue<>::connect(const std::string& name, bool create, size_t buffer_size) {
  std::string name_translated = "ray-{BC200A09-2465-431D-AEC7-2F8530B04535}-" + name;
#if defined(WIN32) || defined(_WIN32)
  name_translated.insert(0, "\\\\.\\pipe\\");
  std::replace(name_translated.begin(), name_translated.end(), '/', '\\');
  if (create) {
    handle_ = reinterpret_cast<intptr_t>(CreateNamedPipeA(name_translated.c_str(), (create ? FILE_FLAG_FIRST_PIPE_INSTANCE : 0) | PIPE_ACCESS_DUPLEX, PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS, 1, static_cast<DWORD>(buffer_size), static_cast<DWORD>(buffer_size), INFINITE, NULL));
  } else {
    handle_ = reinterpret_cast<intptr_t>(CreateFileA(name_translated.c_str(), GENERIC_ALL, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING, 0, NULL));
  }
#else
  name_translated.insert(0, "/tmp/");
  if (!create || mkfifo(name_translated.c_str(), S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH) == 0 || errno == EEXIST) {
    handle_ = open(name_translated.c_str(), O_RDWR);
    if (handle_ == -1) {
      unlink(name_translated.c_str());
    }
  }
#endif
  return handle_ != -1;
}

bool MessageQueue<>::connected() {
  return handle_ != -1;
}

void MessageQueue<>::close() {
  if (connected()) {
#if defined(WIN32) || defined(_WIN32)
    CloseHandle(reinterpret_cast<HANDLE>(handle_));
#else
    ::close(handle_);
#endif
    handle_ = -1;
  }
}

bool MessageQueue<>::send(const unsigned char* object, size_t size) {
  while (size > 0) {
#if defined(WIN32) || defined(_WIN32)
    DWORD transmitted;
    if (!WriteFile(reinterpret_cast<HANDLE>(handle_), object, static_cast<DWORD>(size), &transmitted, NULL)) {
      RAY_LOG(RAY_INFO, "GetLastError() == " << GetLastError());
      break;
    }
#else
    ssize_t transmitted = write(handle_, object, size);
    if (transmitted < 0) {
      RAY_LOG(RAY_INFO, "errno == " << errno);
      break;
    }
#endif
    size -= static_cast<size_t>(transmitted);
    object += static_cast<ptrdiff_t>(transmitted);
    }
  return size == 0;
}

bool MessageQueue<>::receive(unsigned char* object, size_t size) {
  while (size > 0) {
#if defined(WIN32) || defined(_WIN32)
    DWORD transmitted;
    if (!ReadFile(reinterpret_cast<HANDLE>(handle_), object, static_cast<DWORD>(size), &transmitted, NULL)) {
      RAY_LOG(RAY_INFO, "GetLastError() == " << GetLastError());
      break;
    }
#else
    ssize_t transmitted = read(handle_, object, size);
    if (transmitted < 0) {
      RAY_LOG(RAY_INFO, "errno == " << errno);
      break;
    }
#endif
    size -= static_cast<size_t>(transmitted);
    object += static_cast<ptrdiff_t>(transmitted);
  }
  return size == 0;
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
  return std::string("objstore:") + std::to_string(objstoreid_) + std::string(":segment:") + std::to_string(segmentid);
}

MemorySegmentPool::~MemorySegmentPool() {
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
