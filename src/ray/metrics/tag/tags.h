#ifndef RAY_METRICS_TAG_TAGS_H
#define RAY_METRICS_TAG_TAGS_H

#include <map>
#include <set>
#include <string>

namespace ray {

namespace metrics {

/// \class TagKeys
///
/// The immutable class that represents all of the keys in tag.
/// All of the members cannot be changed after creating.
class TagKeys {
 public:
  /// Create a TagKeys object from a set of strings.
  ///
  /// \param keys A set of tag keys.
  TagKeys(const std::set<std::string> &keys = {});

  ~TagKeys() = default;

  /// Get the set of tag keys.
  ///
  /// \return The set of tag keys.
  const std::set<std::string> &GetTagKeys() const {
    return keys_;
  }

  /// Get the id of this TagKeys object.
  ///
  /// \return The id of this TagKeys.
  size_t GetID() const {
    return id_;
  }

 private:
  void DoHash();

 private:
  std::set<std::string> keys_;
  size_t id_{0};
};

/// \class Tags
///
/// The immutable class that represents a set of tags.
/// All of the members cannot be changed after creating.
class Tags {
 public:

  /// Create a Tags object from the given map.
  ///
  /// \param tag_map The map of k-v tags.
  explicit Tags(const std::map<std::string, std::string> &tag_map = {});

  ~Tags() = default;

  /// Get the tags with a map.
  ///
  /// \return The map that contains k-v tags.
  const std::map<std::string, std::string> &GetTags() const {
    return tag_map_;
  }

  /// get the keys of this Tags object.
  ///
  /// \return The TagKeys object that contains all of the keys of this Tags object.
  const TagKeys &GetTagKeys() const {
    return keys_;
  }

  /// Get the id of this Tags object.
  ///
  /// \return The id of this Tags object.
  size_t GetID() const {
    return id_;
  }

 private:
  void DoHash();

  std::map<std::string, std::string> tag_map_;
  size_t id_{0};

  TagKeys keys_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_TAG_TAGS_H
