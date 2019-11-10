#ifndef RAY_COMMON_SCHEDULING_SCHEDULING_IDS_H
#define RAY_COMMON_SCHEDULING_SCHEDULING_IDS_H

#include <iostream>
#include <map>
#include <unordered_map>
#include <string>
#include <iterator>
#include <stdlib.h>
#include <chrono>
#include <vector>
using namespace std;

/// Class to map string IDs to unique inntegre IDs and back.
class ScheduleIds {
  // use unordered_map
  unordered_map<string, int64_t> string_to_int;
  unordered_map<int64_t, string> int_to_string;
  hash<string> hasher;

public:
  ScheduleIds() {};
  ~ScheduleIds() {};

  /// Get integer ID associated with an existing string ID.
  ///
  /// \param String ID.
  /// \return The integer ID associated to the string ID.
  int64_t getIntId(string sid);

  /// Insert a string ID and get the associated integer ID.
  ///
  /// \param String ID to be inserted.
  /// \return The integer ID associated with string ID sid.
  int64_t insertStringId(string sid);

  /// Delete an ID identified by its string format.
  ///
  /// \param ID to be deleted.
  void removeStringId(string sid);

  /// Delete an ID identified by its integer format.
  ///
  /// \param ID to be deleted.
  void removeIntId(int64_t id);

  /// Get number of identifiers.
  int64_t count();
};

#endif // RAY_COMMON_SCHEDULING_SCHEDULING_IDS_H
