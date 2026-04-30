// Phase 4 kill-9 durability harness — writer.
//
// Issues N sequential Put()s to a RocksDB at the given path, optionally
// with WriteOptions::sync = true (production config) or false (negative
// control). Prints "ack:N\n" to stdout after each write returns OK.
//
// The orchestrator (run_kill9.py) reads stdout, picks an offset, and
// SIGKILLs this process. After the kill, the verifier opens the same
// DB and checks every acked key is readable.
//
// We use the rocksdb C++ API directly — not RocksDbStoreClient — because
// Phase 4's claim is about the storage layer's fsync semantics, which
// the StoreClient wrapper does not change. Going through the wrapper
// would add asio plumbing without changing the durability question.
//
// Usage:
//   writer <db_path> <num_writes> <key_prefix> <sync:0|1> [ghost_writes]
//
// ghost_writes (optional, default 0) is a deliberately-broken mode used
// to demonstrate the harness has fault sensitivity: the writer prints
// "ack:N" for the LAST `ghost_writes` keys without actually calling
// db->Put(). The orchestrator should report exactly that many
// acked-but-missing keys when this flag is non-zero.

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <cstdlib>
#include <iostream>
#include <string>

int main(int argc, char **argv) {
  if (argc < 5 || argc > 6) {
    std::cerr << "usage: writer <db_path> <num_writes> <key_prefix> "
                 "<sync:0|1> [ghost_writes]"
              << std::endl;
    return 2;
  }
  const std::string db_path = argv[1];
  const long num_writes = std::atol(argv[2]);
  const std::string key_prefix = argv[3];
  const bool sync_writes = (std::atoi(argv[4]) != 0);
  const long ghost_writes = (argc == 6) ? std::atol(argv[5]) : 0;

  rocksdb::Options opts;
  opts.create_if_missing = true;
  // Match the Phase 3 RocksDbStoreClient defaults so this harness
  // exercises the same write path the production GCS will see.
  opts.compression = rocksdb::kNoCompression;

  rocksdb::DB *db = nullptr;
  auto open_status = rocksdb::DB::Open(opts, db_path, &db);
  if (!open_status.ok()) {
    std::cerr << "open failed: " << open_status.ToString() << std::endl;
    return 1;
  }

  rocksdb::WriteOptions write_opts;
  write_opts.sync = sync_writes;

  const long ghost_threshold = num_writes - ghost_writes;
  for (long i = 0; i < num_writes; ++i) {
    const std::string key = key_prefix + std::to_string(i);
    // Payload is 32 bytes — enough to be non-trivial but not a stress test.
    const std::string value = "payload-" + std::to_string(i) + "-padding-data";
    if (i < ghost_threshold) {
      auto put_status = db->Put(write_opts, key, value);
      if (!put_status.ok()) {
        std::cerr << "put failed at i=" << i << ": " << put_status.ToString()
                  << std::endl;
        delete db;
        return 1;
      }
    }
    // Always print ack: in normal mode this follows a successful Put;
    // in ghost-write mode (i >= ghost_threshold) we deliberately ack
    // a write we never issued, so the orchestrator can confirm it
    // detects the discrepancy.
    std::cout << "ack:" << i << "\n";
    std::cout.flush();
  }

  delete db;
  return 0;
}
