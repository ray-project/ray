// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Mehdi Goli    Codeplay Software Ltd.
// Ralph Potter  Codeplay Software Ltd.
// Luke Iwanski  Codeplay Software Ltd.
// Contact: <eigen@codeplay.com>
// Copyright (C) 2016 Benoit Steiner <benoit.steiner.goog@gmail.com>

//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#if defined(EIGEN_USE_SYCL) && !defined(EIGEN_CXX11_TENSOR_TENSOR_DEVICE_SYCL_H)
#define EIGEN_CXX11_TENSOR_TENSOR_DEVICE_SYCL_H
template<size_t Align> struct CheckAlignStatically {
  static const bool Val= (((Align&(Align-1))==0) && (Align >= sizeof(void *)));
};
template <bool IsAligned, size_t Align>
struct Conditional_Allocate {

  EIGEN_ALWAYS_INLINE static void* conditional_allocate(std::size_t elements) {
    return aligned_alloc(Align, elements);
  }
};
template <size_t Align>
struct Conditional_Allocate<false, Align> {

  EIGEN_ALWAYS_INLINE static void* conditional_allocate(std::size_t elements){
    return malloc(elements);
  }
};
template <typename Scalar, size_t Align = EIGEN_MAX_ALIGN_BYTES, class Allocator = std::allocator<Scalar>>
struct SyclAllocator {
  typedef Scalar value_type;
  typedef typename std::allocator_traits<Allocator>::pointer pointer;
  typedef typename std::allocator_traits<Allocator>::size_type size_type;

  SyclAllocator( ){};
  Scalar* allocate(std::size_t elements) {
    return static_cast<Scalar*>(Conditional_Allocate<CheckAlignStatically<Align>::Val, Align>::conditional_allocate(elements));
  }
  void deallocate(Scalar * p, std::size_t size) { EIGEN_UNUSED_VARIABLE(size); free(p); }
};

namespace Eigen {

#define ConvertToActualTypeSycl(Scalar, buf_acc) static_cast<Scalar*>(static_cast<void*>(((buf_acc.get_pointer().get()))))
#define ConvertToActualSyclOffset(Scalar, offset) offset/sizeof(Scalar)


  template <typename Scalar, typename read_accessor, typename write_accessor> class MemCopyFunctor {
  public:
    MemCopyFunctor(read_accessor src_acc, write_accessor dst_acc, size_t rng, size_t i, size_t offset) : m_src_acc(src_acc), m_dst_acc(dst_acc), m_rng(rng), m_i(i), m_offset(offset) {}

    void operator()(cl::sycl::nd_item<1> itemID) {
      auto src_ptr = ConvertToActualTypeSycl(Scalar, m_src_acc);
      auto dst_ptr = ConvertToActualTypeSycl(Scalar, m_dst_acc);
      auto globalid = itemID.get_global_linear_id();
      if (globalid < m_rng) {
        dst_ptr[globalid + m_i] = src_ptr[globalid + m_offset];
      }
    }

  private:
    read_accessor m_src_acc;
    write_accessor m_dst_acc;
    size_t m_rng;
    size_t m_i;
    size_t m_offset;
  };

template<typename AccType>
  struct memsetkernelFunctor{
   AccType m_acc;
   const ptrdiff_t buff_offset;
   const size_t m_rng, m_c;
   memsetkernelFunctor(AccType acc, const ptrdiff_t buff_offset_, const size_t rng, const size_t c):m_acc(acc),  buff_offset(buff_offset_), m_rng(rng), m_c(c){}
   void operator()(cl::sycl::nd_item<1> itemID) {
     auto globalid=itemID.get_global_linear_id();
     if (globalid< m_rng) m_acc[globalid + buff_offset] = m_c;
   }

  };

struct memsetCghFunctor{
  cl::sycl::buffer<uint8_t, 1, SyclAllocator<uint8_t, EIGEN_MAX_ALIGN_BYTES> >& m_buf;
  const ptrdiff_t& buff_offset;
  const size_t& rng , GRange, tileSize;
  const int  &c;
  memsetCghFunctor(cl::sycl::buffer<uint8_t, 1, SyclAllocator<uint8_t, EIGEN_MAX_ALIGN_BYTES> >& buff,  const ptrdiff_t& buff_offset_, const size_t& rng_,  const size_t& GRange_,  const size_t& tileSize_, const int& c_)
  :m_buf(buff), buff_offset(buff_offset_), rng(rng_), GRange(GRange_), tileSize(tileSize_), c(c_){}

  void operator()(cl::sycl::handler &cgh) const {
    auto buf_acc = m_buf.template get_access<cl::sycl::access::mode::write, cl::sycl::access::target::global_buffer>(cgh);
    typedef decltype(buf_acc) AccType;
    cgh.parallel_for(cl::sycl::nd_range<1>(cl::sycl::range<1>(GRange), cl::sycl::range<1>(tileSize)), memsetkernelFunctor<AccType>(buf_acc, buff_offset, rng, c));
  }
};

//get_devices returns all the available opencl devices. Either use device_selector or exclude devices that computecpp does not support (AMD OpenCL for CPU  and intel GPU)
EIGEN_STRONG_INLINE auto get_sycl_supported_devices()->decltype(cl::sycl::device::get_devices()){
std::vector<cl::sycl::device> supported_devices;
auto plafrom_list =cl::sycl::platform::get_platforms();
for(const auto& platform : plafrom_list){
  auto device_list = platform.get_devices();
  auto platform_name =platform.template get_info<cl::sycl::info::platform::name>();
  std::transform(platform_name.begin(), platform_name.end(), platform_name.begin(), ::tolower);
  for(const auto& device : device_list){
    auto vendor = device.template get_info<cl::sycl::info::device::vendor>();
    std::transform(vendor.begin(), vendor.end(), vendor.begin(), ::tolower);
    bool unsuported_condition = (device.is_cpu() && platform_name.find("amd")!=std::string::npos && vendor.find("apu") == std::string::npos) ||
    (device.is_gpu() && platform_name.find("intel")!=std::string::npos);
    if(!unsuported_condition){
      std::cout << "Platform name "<< platform_name << std::endl;
        supported_devices.push_back(device);
    }
  }
}
return supported_devices;
}

class QueueInterface {
public:
  /// creating device by using cl::sycl::selector or cl::sycl::device both are the same and can be captured through dev_Selector typename
  /// SyclStreamDevice is not owned. it is the caller's responsibility to destroy it.
  template<typename dev_Selector> explicit QueueInterface(const dev_Selector& s):
#ifdef EIGEN_EXCEPTIONS
  m_queue(cl::sycl::queue(s, [&](cl::sycl::exception_list l) {
    for (const auto& e : l) {
      try {
        if (e) {
           exception_caught_ = true;
           std::rethrow_exception(e);
        }
      } catch (cl::sycl::exception e) {
        std::cerr << e.what() << std::endl;
      }
    }
  }))
#else
m_queue(cl::sycl::queue(s, [&](cl::sycl::exception_list l) {
  for (const auto& e : l) {
      if (e) {
         exception_caught_ = true;
         std::cerr << "Error detected Inside Sycl Device."<< std::endl;

      }
  }
}))
#endif
  {}

  /// Allocating device pointer. This pointer is actually an 8 bytes host pointer used as key to access the sycl device buffer.
  /// The reason is that we cannot use device buffer as a pointer as a m_data in Eigen leafNode expressions. So we create a key
  /// pointer to be used in Eigen expression construction. When we convert the Eigen construction into the sycl construction we
  /// use this pointer as a key in our buffer_map and we make sure that we dedicate only one buffer only for this pointer.
  /// The device pointer would be deleted by calling deallocate function.
  EIGEN_STRONG_INLINE void* allocate(size_t num_bytes) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto buf = cl::sycl::buffer<uint8_t,1, SyclAllocator<uint8_t, EIGEN_MAX_ALIGN_BYTES> >(cl::sycl::range<1>(num_bytes));
    auto ptr =buf.get_access<cl::sycl::access::mode::discard_write, cl::sycl::access::target::host_buffer>().get_pointer();
    buf.set_final_data(nullptr);
    buffer_map.insert(std::pair<const uint8_t *, cl::sycl::buffer<uint8_t, 1, SyclAllocator<uint8_t, EIGEN_MAX_ALIGN_BYTES> > >(static_cast<const uint8_t*>(ptr),buf));
    return static_cast<void*>(ptr);
  }

  /// This is used to deallocate the device pointer. p is used as a key inside
  /// the map to find the device buffer and delete it.
  EIGEN_STRONG_INLINE void deallocate(void *p) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = buffer_map.find(static_cast<const uint8_t*>(p));
    if (it != buffer_map.end()) {
      buffer_map.erase(it);
    }
  }

  EIGEN_STRONG_INLINE void deallocate_all() const {
    std::lock_guard<std::mutex> lock(mutex_);
    buffer_map.clear();
  }
  /// The memcpyHostToDevice is used to copy the device only pointer to a host pointer. Using the device
  /// pointer created as a key we find the sycl buffer and get the host accessor with write mode
  /// on it. Then we use the memcpy to copy the data to the host accessor. The first time that
  /// this buffer is accessed, the data will be copied to the device.
  /// In this case we can separate the kernel actual execution from data transfer which is required for benchmark
  /// Also, this is faster as it uses the map_allocator instead of memcpy
  template<typename Index> EIGEN_STRONG_INLINE void memcpyHostToDevice(Index *dst, const Index *src, size_t n) const {
    auto it =find_buffer(dst);
    auto offset =static_cast<const uint8_t*>(static_cast<const void*>(dst))- it->first;
    offset/=sizeof(Index);
    size_t rng, GRange, tileSize;
    parallel_for_setup(n/sizeof(Index), tileSize, rng, GRange);
      auto src_buf = cl::sycl::buffer<uint8_t, 1, cl::sycl::map_allocator<uint8_t> >(static_cast<uint8_t*>(static_cast<void*>(const_cast<Index*>(src))), cl::sycl::range<1>(n));
      m_queue.submit([&](cl::sycl::handler &cgh) {
        auto dst_acc= it->second.template get_access<cl::sycl::access::mode::write, cl::sycl::access::target::global_buffer>(cgh);
        auto src_acc =src_buf.template get_access<cl::sycl::access::mode::read, cl::sycl::access::target::global_buffer>(cgh);
        typedef decltype(src_acc) read_accessor;
        typedef decltype(dst_acc) write_accessor;
        cgh.parallel_for( cl::sycl::nd_range<1>(cl::sycl::range<1>(GRange), cl::sycl::range<1>(tileSize)), MemCopyFunctor<Index, read_accessor, write_accessor>(src_acc, dst_acc, rng, offset, 0));
      });
      synchronize();
  }
  /// The memcpyDeviceToHost is used to copy the data from host to device. Here, in order to avoid double copying the data. We create a sycl
  /// buffer with map_allocator for the destination pointer with a discard_write accessor on it. The lifespan of the buffer is bound to the
  /// lifespan of the memcpyDeviceToHost function. We create a kernel to copy the data, from the device- only source buffer to the destination
  /// buffer with map_allocator on the gpu in parallel. At the end of the function call the destination buffer would be destroyed and the data
  /// would be available on the dst pointer using fast copy technique (map_allocator). In this case we can make sure that we copy the data back
  /// to the cpu only once per function call.
  template<typename Index> EIGEN_STRONG_INLINE void memcpyDeviceToHost(void *dst, const Index *src, size_t n) const {
    auto it =find_buffer(src);
    auto offset =static_cast<const uint8_t*>(static_cast<const void*>(src))- it->first;
    offset/=sizeof(Index);
    size_t rng, GRange, tileSize;
    parallel_for_setup(n/sizeof(Index), tileSize, rng, GRange);
      auto dest_buf = cl::sycl::buffer<uint8_t, 1, cl::sycl::map_allocator<uint8_t> >(static_cast<uint8_t*>(dst), cl::sycl::range<1>(n));
      m_queue.submit([&](cl::sycl::handler &cgh) {
        auto src_acc= it->second.template get_access<cl::sycl::access::mode::read, cl::sycl::access::target::global_buffer>(cgh);
        auto dst_acc =dest_buf.template get_access<cl::sycl::access::mode::discard_write, cl::sycl::access::target::global_buffer>(cgh);
        typedef decltype(src_acc) read_accessor;
        typedef decltype(dst_acc) write_accessor;
        cgh.parallel_for( cl::sycl::nd_range<1>(cl::sycl::range<1>(GRange), cl::sycl::range<1>(tileSize)), MemCopyFunctor<Index, read_accessor, write_accessor>(src_acc, dst_acc, rng, 0, offset));
      });
      synchronize();
  }

  /// the memcpy function
  template<typename Index> EIGEN_STRONG_INLINE void memcpy(void *dst, const Index *src, size_t n) const {
    auto it1 = find_buffer(static_cast<const void*>(src));
    auto it2 = find_buffer(dst);
    auto offset= (static_cast<const uint8_t*>(static_cast<const void*>(src))) - it1->first;
    auto i= (static_cast<const uint8_t*>(dst)) - it2->first;
    offset/=sizeof(Index);
    i/=sizeof(Index);
    size_t rng, GRange, tileSize;
    parallel_for_setup(n/sizeof(Index), tileSize, rng, GRange);
    m_queue.submit([&](cl::sycl::handler &cgh) {
      auto src_acc =it1->second.template get_access<cl::sycl::access::mode::read, cl::sycl::access::target::global_buffer>(cgh);
      auto dst_acc =it2->second.template get_access<cl::sycl::access::mode::write, cl::sycl::access::target::global_buffer>(cgh);
      typedef decltype(src_acc) read_accessor;
      typedef decltype(dst_acc) write_accessor;
      cgh.parallel_for(cl::sycl::nd_range<1>(cl::sycl::range<1>(GRange), cl::sycl::range<1>(tileSize)), MemCopyFunctor<Index, read_accessor, write_accessor>(src_acc, dst_acc, rng, i, offset));
    });
    synchronize();
  }

  EIGEN_STRONG_INLINE void memset(void *data, int c, size_t n) const {
    size_t rng, GRange, tileSize;
    parallel_for_setup(n, tileSize, rng, GRange);
    auto it1 = find_buffer(static_cast<const void*>(data));
    ptrdiff_t buff_offset= (static_cast<const uint8_t*>(data)) - it1->first;
    m_queue.submit(memsetCghFunctor(it1->second, buff_offset, rng, GRange, tileSize, c ));
    synchronize();
  }

  /// Creation of sycl accessor for a buffer. This function first tries to find
  /// the buffer in the buffer_map. If found it gets the accessor from it, if not,
  /// the function then adds an entry by creating a sycl buffer for that particular pointer.
  template <cl::sycl::access::mode AcMd> EIGEN_STRONG_INLINE cl::sycl::accessor<uint8_t, 1, AcMd, cl::sycl::access::target::global_buffer>
  get_sycl_accessor(cl::sycl::handler &cgh, const void* ptr) const {
    return (find_buffer(ptr)->second.template get_access<AcMd, cl::sycl::access::target::global_buffer>(cgh));
  }

  /// Accessing the created sycl device buffer for the device pointer
  EIGEN_STRONG_INLINE cl::sycl::buffer<uint8_t, 1, SyclAllocator<uint8_t, EIGEN_MAX_ALIGN_BYTES> >& get_sycl_buffer(const void * ptr) const {
    return find_buffer(ptr)->second;
  }

  EIGEN_STRONG_INLINE ptrdiff_t get_offset(const void *ptr) const {
    return (static_cast<const uint8_t*>(ptr))-(find_buffer(ptr)->first);
  }

  EIGEN_STRONG_INLINE void synchronize() const {
    m_queue.wait_and_throw(); //pass
  }

  EIGEN_STRONG_INLINE void asynchronousExec() const {
    ///FIXEDME:: currently there is a race condition regarding the asynch scheduler.
    //sycl_queue().throw_asynchronous();// FIXME::does not pass. Temporarily disabled
    m_queue.wait_and_throw(); //pass
  }

  template<typename Index>
  EIGEN_STRONG_INLINE void parallel_for_setup(Index n, Index &tileSize, Index &rng, Index &GRange)  const {
    tileSize =static_cast<Index>(m_queue.get_device(). template get_info<cl::sycl::info::device::max_work_group_size>());
    auto s=  m_queue.get_device().template get_info<cl::sycl::info::device::vendor>();
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    if(m_queue.get_device().is_cpu()){ // intel doesn't allow to use max workgroup size
      tileSize=std::min(static_cast<Index>(256), static_cast<Index>(tileSize));
    }
    rng = n;
    if (rng==0) rng=static_cast<Index>(1);
    GRange=rng;
    if (tileSize>GRange) tileSize=GRange;
    else if(GRange>tileSize){
      Index xMode =  static_cast<Index>(GRange % tileSize);
      if (xMode != 0) GRange += static_cast<Index>(tileSize - xMode);
    }
  }

  /// This is used to prepare the number of threads and also the number of threads per block for sycl kernels
  template<typename Index>
  EIGEN_STRONG_INLINE void parallel_for_setup(Index dim0, Index dim1, Index &tileSize0, Index &tileSize1, Index &rng0, Index &rng1, Index &GRange0, Index &GRange1)  const {
    Index max_workgroup_Size = static_cast<Index>(maxSyclThreadsPerBlock());
    if(m_queue.get_device().is_cpu()){ // intel doesn't allow to use max workgroup size
      max_workgroup_Size=std::min(static_cast<Index>(256), static_cast<Index>(max_workgroup_Size));
    }
    Index pow_of_2 = static_cast<Index>(std::log2(max_workgroup_Size));
    tileSize1 =static_cast<Index>(std::pow(2, static_cast<Index>(pow_of_2/2)));
    rng1=dim1;
    if (rng1==0 ) rng1=static_cast<Index>(1);
    GRange1=rng1;
    if (tileSize1>GRange1) tileSize1=GRange1;
    else if(GRange1>tileSize1){
      Index xMode =  static_cast<Index>(GRange1 % tileSize1);
      if (xMode != 0) GRange1 += static_cast<Index>(tileSize1 - xMode);
    }
    tileSize0 = static_cast<Index>(max_workgroup_Size/tileSize1);
    rng0 = dim0;
    if (rng0==0 ) rng0=static_cast<Index>(1);
    GRange0=rng0;
    if (tileSize0>GRange0) tileSize0=GRange0;
    else if(GRange0>tileSize0){
      Index xMode =  static_cast<Index>(GRange0 % tileSize0);
      if (xMode != 0) GRange0 += static_cast<Index>(tileSize0 - xMode);
    }
  }

  /// This is used to prepare the number of threads and also the number of threads per block for sycl kernels
  template<typename Index>
  EIGEN_STRONG_INLINE void parallel_for_setup(Index dim0, Index dim1,Index dim2, Index &tileSize0, Index &tileSize1, Index &tileSize2, Index &rng0, Index &rng1, Index &rng2, Index &GRange0, Index &GRange1, Index &GRange2)  const {
    Index max_workgroup_Size = static_cast<Index>(maxSyclThreadsPerBlock());
    if(m_queue.get_device().is_cpu()){ // intel doesn't allow to use max workgroup size
      max_workgroup_Size=std::min(static_cast<Index>(256), static_cast<Index>(max_workgroup_Size));
    }
    Index pow_of_2 = static_cast<Index>(std::log2(max_workgroup_Size));
    tileSize2 =static_cast<Index>(std::pow(2, static_cast<Index>(pow_of_2/3)));
    rng2=dim2;
    if (rng2==0 ) rng1=static_cast<Index>(1);
    GRange2=rng2;
    if (tileSize2>GRange2) tileSize2=GRange2;
    else if(GRange2>tileSize2){
      Index xMode =  static_cast<Index>(GRange2 % tileSize2);
      if (xMode != 0) GRange2 += static_cast<Index>(tileSize2 - xMode);
    }
    pow_of_2 = static_cast<Index>(std::log2(static_cast<Index>(max_workgroup_Size/tileSize2)));
    tileSize1 =static_cast<Index>(std::pow(2, static_cast<Index>(pow_of_2/2)));
    rng1=dim1;
    if (rng1==0 ) rng1=static_cast<Index>(1);
    GRange1=rng1;
    if (tileSize1>GRange1) tileSize1=GRange1;
    else if(GRange1>tileSize1){
      Index xMode =  static_cast<Index>(GRange1 % tileSize1);
      if (xMode != 0) GRange1 += static_cast<Index>(tileSize1 - xMode);
    }
    tileSize0 = static_cast<Index>(max_workgroup_Size/(tileSize1*tileSize2));
    rng0 = dim0;
    if (rng0==0 ) rng0=static_cast<Index>(1);
    GRange0=rng0;
    if (tileSize0>GRange0) tileSize0=GRange0;
    else if(GRange0>tileSize0){
      Index xMode =  static_cast<Index>(GRange0 % tileSize0);
      if (xMode != 0) GRange0 += static_cast<Index>(tileSize0 - xMode);
    }
  }

  EIGEN_STRONG_INLINE unsigned long getNumSyclMultiProcessors() const {
    return m_queue.get_device(). template get_info<cl::sycl::info::device::max_compute_units>();
  }

  EIGEN_STRONG_INLINE unsigned long maxSyclThreadsPerBlock() const {
    return m_queue.get_device(). template get_info<cl::sycl::info::device::max_work_group_size>();
  }

  /// No need for sycl it should act the same as CPU version
  EIGEN_STRONG_INLINE int majorDeviceVersion() const { return 1; }

  EIGEN_STRONG_INLINE unsigned long maxSyclThreadsPerMultiProcessor() const {
    // OpenCL doesn't have such concept
    return 2;
  }

  EIGEN_STRONG_INLINE size_t sharedMemPerBlock() const {
    return m_queue.get_device(). template get_info<cl::sycl::info::device::local_mem_size>();
  }

  EIGEN_STRONG_INLINE cl::sycl::queue& sycl_queue() const { return m_queue;}

  // This function checks if the runtime recorded an error for the
  // underlying stream device.
  EIGEN_STRONG_INLINE bool ok() const {
    if (!exception_caught_) {
      m_queue.wait_and_throw();
    }
    return !exception_caught_;
  }

  // destructor
  ~QueueInterface() { buffer_map.clear(); }

private:
  /// class members:
  bool exception_caught_ = false;

  mutable std::mutex mutex_;

  /// std::map is the container used to make sure that we create only one buffer
  /// per pointer. The lifespan of the buffer now depends on the lifespan of SyclDevice.
  /// If a non-read-only pointer is needed to be accessed on the host we should manually deallocate it.
  mutable std::map<const uint8_t *, cl::sycl::buffer<uint8_t, 1, SyclAllocator<uint8_t, EIGEN_MAX_ALIGN_BYTES> > > buffer_map;
  /// sycl queue
  mutable cl::sycl::queue m_queue;

  EIGEN_STRONG_INLINE std::map<const uint8_t *, cl::sycl::buffer<uint8_t,1, SyclAllocator<uint8_t, EIGEN_MAX_ALIGN_BYTES> > >::iterator find_buffer(const void* ptr) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it1 = buffer_map.find(static_cast<const uint8_t*>(ptr));
    if (it1 != buffer_map.end()){
      return it1;
    }
    else{
      for(std::map<const uint8_t *, cl::sycl::buffer<uint8_t,1, SyclAllocator<uint8_t, EIGEN_MAX_ALIGN_BYTES> > >::iterator it=buffer_map.begin(); it!=buffer_map.end(); ++it){
        auto size = it->second.get_size();
        if((it->first <  (static_cast<const uint8_t*>(ptr))) && ((static_cast<const uint8_t*>(ptr)) < (it->first + size)) ) return it;
      }
    }
    std::cerr << "No sycl buffer found. Make sure that you have allocated memory for your buffer by calling malloc-ed function."<< std::endl;
    abort();
  }
};

// Here is a sycl deviuce struct which accept the sycl queue interface
// as an input
struct SyclDevice {
  // class member.
  QueueInterface* m_queue_stream;
  /// QueueInterface is not owned. it is the caller's responsibility to destroy it.
  explicit SyclDevice(QueueInterface* queue_stream) : m_queue_stream(queue_stream){}

  // get sycl accessor
  template <cl::sycl::access::mode AcMd> EIGEN_STRONG_INLINE cl::sycl::accessor<uint8_t, 1, AcMd, cl::sycl::access::target::global_buffer>
  get_sycl_accessor(cl::sycl::handler &cgh, const void* ptr) const {
    return m_queue_stream->template get_sycl_accessor<AcMd>(cgh, ptr);
  }

  /// Accessing the created sycl device buffer for the device pointer
  EIGEN_STRONG_INLINE cl::sycl::buffer<uint8_t, 1, SyclAllocator<uint8_t, EIGEN_MAX_ALIGN_BYTES> >& get_sycl_buffer(const void * ptr) const {
    return m_queue_stream->get_sycl_buffer(ptr);
  }

  /// This is used to prepare the number of threads and also the number of threads per block for sycl kernels
  template<typename Index>
  EIGEN_STRONG_INLINE void parallel_for_setup(Index n, Index &tileSize, Index &rng, Index &GRange)  const {
  m_queue_stream->parallel_for_setup(n, tileSize, rng, GRange);
  }

  /// This is used to prepare the number of threads and also the number of threads per block for sycl kernels
  template<typename Index>
  EIGEN_STRONG_INLINE void parallel_for_setup(Index dim0, Index dim1, Index &tileSize0, Index &tileSize1, Index &rng0, Index &rng1, Index &GRange0, Index &GRange1)  const {
    m_queue_stream->parallel_for_setup(dim0, dim1, tileSize0, tileSize1, rng0, rng1, GRange0, GRange1);
  }

  /// This is used to prepare the number of threads and also the number of threads per block for sycl kernels
  template<typename Index>
  EIGEN_STRONG_INLINE void parallel_for_setup(Index dim0, Index dim1,Index dim2, Index &tileSize0, Index &tileSize1, Index &tileSize2, Index &rng0, Index &rng1, Index &rng2, Index &GRange0, Index &GRange1, Index &GRange2)  const {
    m_queue_stream->parallel_for_setup(dim0, dim1, dim2, tileSize0, tileSize1, tileSize2, rng0, rng1, rng2, GRange0, GRange1, GRange2);

  }
  /// allocate device memory
  EIGEN_STRONG_INLINE void *allocate(size_t num_bytes) const {
      return m_queue_stream->allocate(num_bytes);
  }
  /// deallocate device memory
  EIGEN_STRONG_INLINE void deallocate(void *p) const {
     m_queue_stream->deallocate(p);
   }

  // some runtime conditions that can be applied here
  EIGEN_STRONG_INLINE bool isDeviceSuitable() const { return true; }

  /// the memcpy function
  template<typename Index> EIGEN_STRONG_INLINE void memcpy(void *dst, const Index *src, size_t n) const {
    m_queue_stream->memcpy(dst,src,n);
  }

  EIGEN_STRONG_INLINE ptrdiff_t get_offset(const void *ptr) const {
    return m_queue_stream->get_offset(ptr);

  }
// memcpyHostToDevice
  template<typename Index> EIGEN_STRONG_INLINE void memcpyHostToDevice(Index *dst, const Index *src, size_t n) const {
     m_queue_stream->memcpyHostToDevice(dst,src,n);
  }
/// here is the memcpyDeviceToHost
  template<typename Index> EIGEN_STRONG_INLINE void memcpyDeviceToHost(void *dst, const Index *src, size_t n) const {
    m_queue_stream->memcpyDeviceToHost(dst,src,n);
  }
  /// Here is the implementation of memset function on sycl.
  EIGEN_STRONG_INLINE void memset(void *data, int c, size_t n) const {
    m_queue_stream->memset(data,c,n);
  }
  /// returning the sycl queue
  EIGEN_STRONG_INLINE cl::sycl::queue& sycl_queue() const { return m_queue_stream->sycl_queue();}

  EIGEN_STRONG_INLINE size_t firstLevelCacheSize() const {
    // FIXME
    return 48*1024;
  }

  EIGEN_STRONG_INLINE size_t lastLevelCacheSize() const {
    // We won't try to take advantage of the l2 cache for the time being, and
    // there is no l3 cache on sycl devices.
    return firstLevelCacheSize();
  }
  EIGEN_STRONG_INLINE unsigned long getNumSyclMultiProcessors() const {
    return m_queue_stream->getNumSyclMultiProcessors();
  }
  EIGEN_STRONG_INLINE unsigned long maxSyclThreadsPerBlock() const {
    return m_queue_stream->maxSyclThreadsPerBlock();
  }
  EIGEN_STRONG_INLINE unsigned long maxSyclThreadsPerMultiProcessor() const {
    // OpenCL doesn't have such concept
    return m_queue_stream->maxSyclThreadsPerMultiProcessor();
  //  return stream_->deviceProperties().maxThreadsPerMultiProcessor;
  }
  EIGEN_STRONG_INLINE size_t sharedMemPerBlock() const {
    return m_queue_stream->sharedMemPerBlock();
  }
  /// No need for sycl it should act the same as CPU version
  EIGEN_STRONG_INLINE int majorDeviceVersion() const { return m_queue_stream->majorDeviceVersion(); }

  EIGEN_STRONG_INLINE void synchronize() const {
    m_queue_stream->synchronize(); //pass
  }

  EIGEN_STRONG_INLINE void asynchronousExec() const {
    m_queue_stream->asynchronousExec();
  }
  // This function checks if the runtime recorded an error for the
  // underlying stream device.
  EIGEN_STRONG_INLINE bool ok() const {
    return m_queue_stream->ok();
  }
};
// This is used as a distingushable device inside the kernel as the sycl device class is not Standard layout.
// This is internal and must not be used by user. This dummy device allow us to specialise the tensor evaluator
// inside the kernel. So we can have two types of eval for host and device. This is required for TensorArgMax operation
struct SyclKernelDevice:DefaultDevice{};

}  // end namespace Eigen

#endif  // EIGEN_CXX11_TENSOR_TENSOR_DEVICE_SYCL_H
