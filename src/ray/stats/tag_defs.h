#ifndef RAY_STATS_TAG_DEFS_H_
#define RAY_STATS_TAG_DEFS_H_

/// The definitions of tag keys that you can use every where.
/// You can follow these examples to define and register your tag keys.

using TagKeyType = opencensus::tags::TagKey;

static TagKeyType JobNameKey = opencensus::tags::TagKey::Register("JobName");

static TagKeyType CustomKey =  opencensus::tags::TagKey::Register("CustomKey");

static TagKeyType NodeAddressKey = opencensus::tags::TagKey::Register("NodeAddress");

static TagKeyType VersionKey = opencensus::tags::TagKey::Register("Version");

static TagKeyType LanguageKey = opencensus::tags::TagKey::Register("Language");

static TagKeyType WorkerPidKey = opencensus::tags::TagKey::Register("WorkerPid");

/// The definitions of global tag.
static std::vector<std::pair<opencensus::tags::TagKey, std::string>> GlobalTags = {
    {JobNameKey, "raylet"},
    {VersionKey, "0.6.5"}
};

#endif // RAY_STATS_TAG_DEFS_H_
