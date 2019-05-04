#ifndef RAY_STATS_TAG_DEFS_H
#define RAY_STATS_TAG_DEFS_H

/// The definitions of tag keys that you can use every where.
/// You can follow these examples to define and register your tag keys.

using TagKeyType = opencensus::tags::TagKey;
using TagsType = std::vector<std::pair<opencensus::tags::TagKey, std::string>>;

static const TagKeyType JobNameKey = TagKeyType::Register("JobName");

static const TagKeyType CustomKey = TagKeyType::Register("CustomKey");

static const TagKeyType NodeAddressKey = TagKeyType::Register("NodeAddress");

static const TagKeyType VersionKey = TagKeyType::Register("Version");

static const TagKeyType LanguageKey = TagKeyType::Register("Language");

static const TagKeyType WorkerPidKey = TagKeyType::Register("WorkerPid");

static const TagKeyType DriverPidKey = TagKeyType::Register("DriverPid");

static const TagKeyType ResourceNameKey = TagKeyType::Register("ResourceName");

static const TagKeyType ValueTypeKey = TagKeyType::Register("ValueType");

#endif  // RAY_STATS_TAG_DEFS_H
