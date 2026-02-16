// Copyright 2017 The Ray Authors.
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

#include "ray/raylet/local_disk_spiller.h"

#include <cstring>
#include <filesystem>
#include <fstream>

#include "absl/container/flat_hash_set.h"
#include "ray/util/logging.h"

namespace ray {

namespace raylet {

namespace {
const std::string kSpillDirPrefix = "ray_spilled_objects_";
}

LocalDiskSpiller::LocalDiskSpiller(const std::vector<std::string> &spill_directories,
                                   const NodeID &node_id,
                                   int num_io_threads,
                                   instrumented_io_context &main_io_service,
                                   RestoreObjectToPlasmaFn restore_to_plasma)
    : main_io_service_(main_io_service),
      restore_to_plasma_(std::move(restore_to_plasma)),
      work_guard_(boost::asio::make_work_guard(io_context_)) {
  RAY_CHECK(!spill_directories.empty()) << "At least one spill directory is required.";
  RAY_CHECK(num_io_threads > 0) << "num_io_threads must be positive.";

  // Create per-node subdirectories.
  std::string node_hex = node_id.Hex();
  for (const auto &base_dir : spill_directories) {
    std::string dir = base_dir + "/" + kSpillDirPrefix + node_hex;
    std::filesystem::create_directories(dir);
    spill_dirs_.push_back(std::move(dir));
  }

  // Start IO threads.
  for (int i = 0; i < num_io_threads; ++i) {
    io_threads_.emplace_back([this] { io_context_.run(); });
  }
  RAY_LOG(INFO) << "LocalDiskSpiller started with " << num_io_threads
                << " IO threads across " << spill_dirs_.size() << " directories.";
}

LocalDiskSpiller::~LocalDiskSpiller() {
  work_guard_.reset();
  io_context_.stop();
  for (auto &t : io_threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
}

void LocalDiskSpiller::WriteUint64LE(std::ostream &os, uint64_t value) {
  char buf[sizeof(uint64_t)];
  for (size_t i = 0; i < sizeof(uint64_t); ++i) {
    buf[i] = static_cast<char>(value & 0xFF);
    value >>= 8;
  }
  os.write(buf, sizeof(buf));
}

const std::string &LocalDiskSpiller::NextSpillDirectory() {
  uint64_t idx = dir_index_.fetch_add(1, std::memory_order_relaxed);
  return spill_dirs_[idx % spill_dirs_.size()];
}

void LocalDiskSpiller::SpillObjects(
    const std::vector<ObjectID> &object_ids,
    const std::vector<const RayObject *> &objects,
    const std::vector<rpc::Address> &owner_addresses,
    std::function<void(const Status &, std::vector<std::string> urls)> callback) {
  RAY_CHECK_EQ(object_ids.size(), objects.size());
  RAY_CHECK_EQ(object_ids.size(), owner_addresses.size());

  // Capture the data we need for the IO thread. We must copy object data now
  // because the RayObject pointers may not be valid after this call returns.
  struct ObjectData {
    std::string serialized_address;
    std::string metadata;
    std::string data;
  };
  auto object_data = std::make_shared<std::vector<ObjectData>>();
  object_data->reserve(object_ids.size());
  for (size_t i = 0; i < object_ids.size(); ++i) {
    ObjectData od;
    od.serialized_address = owner_addresses[i].SerializeAsString();
    const auto *obj = objects[i];
    if (obj->HasMetadata() && obj->GetMetadata()->Size() > 0) {
      od.metadata.assign(reinterpret_cast<const char *>(obj->GetMetadata()->Data()),
                         obj->GetMetadata()->Size());
    }
    auto data_buf = obj->GetData();
    if (data_buf && data_buf->Size() > 0) {
      od.data.assign(reinterpret_cast<const char *>(data_buf->Data()), data_buf->Size());
    }
    object_data->push_back(std::move(od));
  }

  // Choose directory and file name.
  const std::string &dir = NextSpillDirectory();
  uint64_t counter = file_counter_.fetch_add(1, std::memory_order_relaxed);
  std::string filename =
      std::to_string(counter) + "-multi-" + std::to_string(object_ids.size());
  std::string filepath = dir + "/" + filename;

  // Post the file I/O to the IO thread pool.
  boost::asio::post(
      io_context_,
      [this,
       filepath,
       object_data,
       num_objects = object_ids.size(),
       callback = std::move(callback)]() mutable {
        std::vector<std::string> urls;
        urls.reserve(num_objects);

        std::ofstream ofs(filepath, std::ios::binary);
        if (!ofs) {
          auto status =
              Status::IOError("Failed to open spill file for writing: " + filepath);
          boost::asio::post(main_io_service_,
                            [callback = std::move(callback), status]() mutable {
                              callback(status, {});
                            });
          return;
        }

        uint64_t offset = 0;
        for (size_t i = 0; i < num_objects; ++i) {
          const auto &od = (*object_data)[i];
          uint64_t address_size = od.serialized_address.size();
          uint64_t metadata_size = od.metadata.size();
          uint64_t data_size = od.data.size();

          WriteUint64LE(ofs, address_size);
          WriteUint64LE(ofs, metadata_size);
          WriteUint64LE(ofs, data_size);
          ofs.write(od.serialized_address.data(), address_size);
          ofs.write(od.metadata.data(), metadata_size);
          ofs.write(od.data.data(), data_size);

          uint64_t object_size = 24 + address_size + metadata_size + data_size;
          std::string url = filepath + "?offset=" + std::to_string(offset) +
                            "&size=" + std::to_string(object_size);
          urls.push_back(std::move(url));
          offset += object_size;
        }

        ofs.flush();
        if (!ofs) {
          auto status = Status::IOError("Failed to write spill file: " + filepath);
          boost::asio::post(main_io_service_,
                            [callback = std::move(callback), status]() mutable {
                              callback(status, {});
                            });
          return;
        }
        ofs.close();

        boost::asio::post(
            main_io_service_,
            [callback = std::move(callback), urls = std::move(urls)]() mutable {
              callback(Status::OK(), std::move(urls));
            });
      });
}

void LocalDiskSpiller::RestoreSpilledObject(
    const ObjectID &object_id,
    const std::string &object_url,
    std::function<void(const Status &, int64_t bytes_restored)> callback) {
  boost::asio::post(
      io_context_,
      [this, object_id, object_url, callback = std::move(callback)]() mutable {
        // Parse URL: {filepath}?offset={offset}&size={size}
        std::string file_path;
        uint64_t object_offset = 0;

        auto qpos = object_url.find('?');
        if (qpos == std::string::npos) {
          auto status =
              Status::IOError("Malformed spill URL (no query string): " + object_url);
          boost::asio::post(main_io_service_,
                            [callback = std::move(callback), status]() mutable {
                              callback(status, 0);
                            });
          return;
        }
        file_path = object_url.substr(0, qpos);
        std::string query = object_url.substr(qpos + 1);

        // Parse offset=<val>&size=<val>
        auto parse_param = [&](const std::string &q,
                               const std::string &key) -> std::optional<uint64_t> {
          std::string prefix = key + "=";
          auto pos = q.find(prefix);
          if (pos == std::string::npos) return std::nullopt;
          auto start = pos + prefix.size();
          auto end = q.find('&', start);
          try {
            return std::stoull(q.substr(start, end - start));
          } catch (...) {
            return std::nullopt;
          }
        };

        auto offset_opt = parse_param(query, "offset");
        auto size_opt = parse_param(query, "size");
        if (!offset_opt || !size_opt) {
          auto status =
              Status::IOError("Malformed spill URL (bad offset/size): " + object_url);
          boost::asio::post(main_io_service_,
                            [callback = std::move(callback), status]() mutable {
                              callback(status, 0);
                            });
          return;
        }
        object_offset = *offset_opt;
        // size_opt is validated but not used; we read the header to get actual sizes.

        // Read the object from disk.
        std::ifstream ifs(file_path, std::ios::binary);
        if (!ifs) {
          auto status = Status::IOError("Failed to open spill file: " + file_path);
          boost::asio::post(main_io_service_,
                            [callback = std::move(callback), status]() mutable {
                              callback(status, 0);
                            });
          return;
        }

        ifs.seekg(object_offset);
        if (!ifs) {
          auto status = Status::IOError("Failed to seek in spill file: " + file_path);
          boost::asio::post(main_io_service_,
                            [callback = std::move(callback), status]() mutable {
                              callback(status, 0);
                            });
          return;
        }

        // Read header: address_size, metadata_size, data_size (each 8 bytes LE).
        auto read_uint64_le = [&ifs]() -> std::optional<uint64_t> {
          char buf[8];
          if (!ifs.read(buf, 8)) return std::nullopt;
          uint64_t val = 0;
          for (int i = 7; i >= 0; --i) {
            val = (val << 8) | static_cast<uint8_t>(buf[i]);
          }
          return val;
        };

        auto address_size_opt = read_uint64_le();
        auto metadata_size_opt = read_uint64_le();
        auto data_size_opt = read_uint64_le();
        if (!address_size_opt || !metadata_size_opt || !data_size_opt) {
          auto status =
              Status::IOError("Failed to read object header from: " + object_url);
          boost::asio::post(main_io_service_,
                            [callback = std::move(callback), status]() mutable {
                              callback(status, 0);
                            });
          return;
        }

        uint64_t address_size = *address_size_opt;
        uint64_t metadata_size = *metadata_size_opt;
        uint64_t data_size = *data_size_opt;

        // Read owner address.
        std::string address_str(address_size, '\0');
        if (!ifs.read(address_str.data(), address_size)) {
          auto status =
              Status::IOError("Failed to read owner address from: " + object_url);
          boost::asio::post(main_io_service_,
                            [callback = std::move(callback), status]() mutable {
                              callback(status, 0);
                            });
          return;
        }
        rpc::Address owner_address;
        if (!owner_address.ParseFromString(address_str)) {
          auto status =
              Status::IOError("Failed to parse owner address from: " + object_url);
          boost::asio::post(main_io_service_,
                            [callback = std::move(callback), status]() mutable {
                              callback(status, 0);
                            });
          return;
        }

        // Read metadata.
        std::shared_ptr<Buffer> metadata_buf;
        if (metadata_size > 0) {
          auto buf = std::make_shared<LocalMemoryBuffer>(metadata_size);
          if (!ifs.read(reinterpret_cast<char *>(buf->Data()), metadata_size)) {
            auto status = Status::IOError("Failed to read metadata from: " + object_url);
            boost::asio::post(main_io_service_,
                              [callback = std::move(callback), status]() mutable {
                                callback(status, 0);
                              });
            return;
          }
          metadata_buf = std::move(buf);
        }

        // Read data.
        std::shared_ptr<Buffer> data_buf;
        if (data_size > 0) {
          auto buf = std::make_shared<LocalMemoryBuffer>(data_size);
          if (!ifs.read(reinterpret_cast<char *>(buf->Data()), data_size)) {
            auto status =
                Status::IOError("Failed to read object data from: " + object_url);
            boost::asio::post(main_io_service_,
                              [callback = std::move(callback), status]() mutable {
                                callback(status, 0);
                              });
            return;
          }
          data_buf = std::move(buf);
        }

        int64_t bytes_restored = static_cast<int64_t>(data_size + metadata_size);

        // Post the plasma store write back to the main thread.
        boost::asio::post(
            main_io_service_,
            [this,
             object_id,
             owner_address = std::move(owner_address),
             data_buf = std::move(data_buf),
             metadata_buf = std::move(metadata_buf),
             bytes_restored,
             callback = std::move(callback)]() mutable {
              auto status = restore_to_plasma_(
                  object_id, owner_address, std::move(data_buf), std::move(metadata_buf));
              callback(status, bytes_restored);
            });
      });
}

void LocalDiskSpiller::DeleteSpilledObjects(
    const std::vector<std::string> &urls, std::function<void(const Status &)> callback) {
  // Copy URLs since we're posting to another thread.
  auto urls_copy = std::make_shared<std::vector<std::string>>(urls);
  boost::asio::post(
      io_context_,
      [this, urls_copy = std::move(urls_copy), callback = std::move(callback)]() mutable {
        // Collect unique file paths from URLs (multiple objects may share a file).
        absl::flat_hash_set<std::string> file_paths;
        for (const auto &url : *urls_copy) {
          auto qpos = url.find('?');
          if (qpos != std::string::npos) {
            file_paths.insert(url.substr(0, qpos));
          } else {
            file_paths.insert(url);
          }
        }

        Status result = Status::OK();
        for (const auto &path : file_paths) {
          std::error_code ec;
          if (!std::filesystem::remove(path, ec) && ec) {
            RAY_LOG(WARNING) << "Failed to delete spilled object file " << path << ": "
                             << ec.message();
            result = Status::IOError("Failed to delete: " + path + ": " + ec.message());
          }
        }

        boost::asio::post(
            main_io_service_,
            [callback = std::move(callback), result]() mutable { callback(result); });
      });
}

}  // namespace raylet

}  // namespace ray
