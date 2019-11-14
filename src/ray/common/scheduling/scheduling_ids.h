#ifndef RAY_COMMON_SCHEDULING_SCHEDULING_IDS_H
#define RAY_COMMON_SCHEDULING_SCHEDULING_IDS_H

#include <unordered_map>
#include <string>
#include <iterator>
#include <stdlib.h>
#include <gtest/gtest_prod.h>

/// Class to map string IDs to unique integer IDs and back.
class ScheduleIds {
  std::unordered_map<string, int64_t> string_to_int;
  std::unordered_map<int64_t, string> int_to_string;
  std::hash<string> hasher;

  FRIEND_TEST(SchedulingTest, SchedulingIdTest);

public:
  ScheduleIds() {};
  ~ScheduleIds() {};

  /// Get integer ID associated with an existing string ID.
  ///
  /// \param String ID.
  /// \return The integer ID associated with the given string ID.
  int64_t getIdByInt(string sid);

  /// Insert a string ID and get the associated integer ID.
  ///
  /// \param String ID to be inserted.
  /// \return The integer ID associated with string ID sid.
  int64_t insertIdByString(string sid, bool test = false);

  /// Delete an ID identified by its string format.
  ///
  /// \param ID to be deleted.
  void removeIdByString(string sid);

  /// Delete an ID identified by its integer format.
  ///
  /// \param ID to be deleted.
  void removeIdByInt(int64_t id);

  /// Get number of identifiers.
  int64_t count();
};

#endif // RAY_COMMON_SCHEDULING_SCHEDULING_IDS_H
