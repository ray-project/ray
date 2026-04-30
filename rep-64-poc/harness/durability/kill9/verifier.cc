// Phase 4 kill-9 durability harness — verifier.
//
// Opens the RocksDB at the given path read-only and looks up every key
// in the range [0, num_writes). Prints "found:N\n" or "missing:N\n"
// per key. The orchestrator compares this output against the writer's
// ack log: any key the writer acked but the verifier reports missing
// is a durability bug on the substrate under test.
//
// Usage:
//   verifier <db_path> <num_writes> <key_prefix>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <cstdlib>
#include <iostream>
#include <string>

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "usage: verifier <db_path> <num_writes> <key_prefix>"
              << std::endl;
    return 2;
  }
  const std::string db_path = argv[1];
  const long num_writes = std::atol(argv[2]);
  const std::string key_prefix = argv[3];

  rocksdb::Options opts;
  opts.create_if_missing = false;
  rocksdb::DB *db = nullptr;
  auto open_status = rocksdb::DB::OpenForReadOnly(opts, db_path, &db);
  if (!open_status.ok()) {
    std::cerr << "open failed: " << open_status.ToString() << std::endl;
    return 1;
  }

  long found = 0;
  long missing = 0;
  for (long i = 0; i < num_writes; ++i) {
    const std::string key = key_prefix + std::to_string(i);
    std::string value;
    auto get_status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (get_status.ok()) {
      std::cout << "found:" << i << "\n";
      ++found;
    } else if (get_status.IsNotFound()) {
      std::cout << "missing:" << i << "\n";
      ++missing;
    } else {
      std::cerr << "unexpected get error at i=" << i << ": "
                << get_status.ToString() << std::endl;
      delete db;
      return 1;
    }
  }

  std::cerr << "verifier: found=" << found << " missing=" << missing
            << std::endl;
  delete db;
  return 0;
}
