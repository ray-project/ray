#ifndef RAY_COMMON_SCHEDULING_SCHEDULING_IDS_H
#define RAY_COMMON_SCHEDULING_SCHEDULING_IDS_H

#include <string>
#include <unordered_map>

/// Class to map string IDs to unique integer IDs and back.
class StringIdMap {
  std::unordered_map<std::string, int64_t> string_to_int_;
  std::unordered_map<int64_t, std::string> int_to_string_;
  std::hash<std::string> hasher_;

 public:
  StringIdMap(){};
  ~StringIdMap(){};

  /// Get integer ID associated with an existing string ID.
  ///
  /// \param String ID.
  /// \return The integer ID associated with the given string ID.
  int64_t get(const std::string sid);

  /// Insert a string ID and get the associated integer ID.
  ///
  /// \param String ID to be inserted.
  /// \param test: if "true" it specifies that the range of
  ///        IDs is limited to 0..10 for testing purposes.
  /// \return The integer ID associated with string ID sid.
  int64_t insert(const std::string sid, bool test = false);

  /// Delete an ID identified by its string format.
  ///
  /// \param ID to be deleted.
  void remove(const std::string sid);

  /// Delete an ID identified by its integer format.
  ///
  /// \param ID to be deleted.
  void remove(int64_t id);

  /// Get number of identifiers.
  int64_t count();
};

#endif  // RAY_COMMON_SCHEDULING_SCHEDULING_IDS_H
