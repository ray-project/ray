#ifndef RAY_IPC_H
#define RAY_IPC_H

#include <iostream>
#include <limits>

#if defined(WIN32) || defined(_WIN32)
#include <boost/interprocess/detail/windows_intermodule_singleton.hpp>
namespace boost {
  namespace interprocess {
    namespace ipcdetail {
      struct windows_bootstamp;
      template<>
      class windows_intermodule_singleton<windows_bootstamp> {
      public:
        static windows_bootstamp get();
      };
    }
  }
}
#endif

#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>

#ifndef __APPLE__
  #include <arrow/api.h>
  #include <arrow/ipc/memory.h>
#endif

#include "ray/ray.h"

namespace bip = boost::interprocess;

// Methods for inter process communication (abstracts from the shared memory implementation)

// Message Queues: Exchanging objects of type T between processes on a node

template<typename T = void>
class MessageQueue;

template<>
class MessageQueue<> {
public:
  ~MessageQueue();
  MessageQueue();
  MessageQueue(MessageQueue&& other);
  MessageQueue& operator=(MessageQueue&& other);
  bool connected();
protected:
  bool connect(const std::string& name, bool create, size_t message_size, size_t message_capacity);
  bool send(const void* object, size_t size);;
  bool receive(void* object, size_t size);
private:
  std::string name_;
  bool create_;
  std::unique_ptr<bip::message_queue> queue_;
};

template<typename T>
class MessageQueue : public MessageQueue<> {
public:
  bool connect(const std::string& name, bool create, size_t capacity = 100) { return MessageQueue<>::connect(name, create, sizeof(T), capacity); }
  bool send(const T* object) { return MessageQueue<>::send(object, sizeof(*object)); };
  bool receive(T* object) { return MessageQueue<>::receive(object, sizeof(*object)); }
};

// Object Queues

// For communicating between object store and workers, the following
// messages can be sent:

// ALLOC: workerid, objref, size -> objhandle:
// worker requests an allocation from the object store
// GET: workerid, objref -> objhandle:
// worker requests an object from the object store
// WORKER_DONE: workerid, objref -> ():
// worker tells the object store that an object has been finalized
// ALIAS_DONE: objref -> ():
// objstore tells itself that it has finalized something (perhaps an alias)

enum ObjRequestType {ALLOC = 0, GET = 1, WORKER_DONE = 2, ALIAS_DONE = 3};

struct ObjRequest {
  WorkerId workerid; // worker that sends the request
  ObjRequestType type; // do we want to allocate a new object or get a handle?
  ObjRef objref; // object reference of the object to be returned/allocated
  int64_t size; // if allocate, that's the size of the object
  int64_t metadata_offset; // if sending 'WORKER_DONE', that's the location of the metadata relative to the beginning of the object
};

typedef size_t SegmentId; // index into a memory segment table
typedef bip::managed_shared_memory::handle_t IpcPointer;

// Object handle: Handle to object that can be passed around between processes
// that are connected to the same object store

class ObjHandle {
public:
  ObjHandle(SegmentId segmentid = 0, size_t size = 0, IpcPointer ipcpointer = IpcPointer(), size_t metadata_offset = 0);
  SegmentId segmentid() { return segmentid_; }
  size_t size() { return size_; }
  IpcPointer ipcpointer() { return ipcpointer_; }
  size_t metadata_offset() { return metadata_offset_; }
  void set_metadata_offset(size_t metadata_offset) {metadata_offset_ = metadata_offset; }
private:
  SegmentId segmentid_; // which shared memory file the object is stored in
  IpcPointer ipcpointer_; // pointer to the beginning of the object, exchangeable between processes
  size_t size_; // total size of the object
  size_t metadata_offset_; // offset of the metadata that describes this object
};

#ifndef __APPLE__

class BufferMemorySource: public arrow::ipc::MemorySource {
public:
  BufferMemorySource(uint8_t* data, int64_t capacity) : data_(data), capacity_(capacity), size_(0) {}
  virtual arrow::Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<arrow::Buffer>* out);
  virtual arrow::Status Close();
  virtual arrow::Status Write(int64_t position, const uint8_t* data, int64_t nbytes);
  virtual int64_t Size() const;
 private:
  uint8_t* data_;
  int64_t capacity_;
  int64_t size_;
};

#endif

// Memory segment pool: A collection of shared memory segments
// used in two modes:
// \item on the object store it is used with create = true, in this case the
// segments are allocated
// \item on the worker it is used in open mode, with create = false, in this case
// the segments, which have been created by the object store, are just mapped
// into memory

enum SegmentStatusType {UNOPENED = 0, OPENED = 1, CLOSED = 2};

class MemorySegmentPool {
public:
  MemorySegmentPool(ObjStoreId objstoreid, bool create); // can be used in two modes: create mode and open mode (see above)
  ~MemorySegmentPool();
  ObjHandle allocate(size_t nbytes); // allocate memory, potentially creating a new segment (only run on object store)
  void deallocate(ObjHandle pointer); // deallocate object, potentially deallocating a new segment (only run on object store)
  uint8_t* get_address(ObjHandle pointer); // get address of shared object
  std::string get_segment_name(SegmentId segmentid); // get the name of a segment
  void unmap_segment(SegmentId segmentid); // unmap a memory segment from a client (only to be called by clients)
  void destroy_segments();
  void objstore_memcheck(int64_t size);
private:
  void open_segment(SegmentId segmentid, size_t size = 0); // create a segment or map an existing one into memory
  void close_segment(SegmentId segmentid); // close a segment
  bool create_mode_; // true in the object stores, false on the workers
  ObjStoreId objstoreid_; // the identity of the associated object store
  size_t page_size_ = bip::mapped_region::get_page_size();
  std::vector<std::pair<std::unique_ptr<bip::managed_shared_memory>, SegmentStatusType> > segments_;
};

#endif
