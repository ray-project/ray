#ifndef RAY_STATS_TAG_DEFS_H_
#define RAY_STATS_TAG_DEFS_H_

/// The definitions of tag keys that you can use every where.
/// You can follow these examples to define and register your tag keys.

static opencensus::tags::TagKey JobNameKey = opencensus::tags::TagKey::Register("JobName");

static opencensus::tags::TagKey CustomKey =  opencensus::tags::TagKey::Register("CustomKey");

static opencensus::tags::TagKey NodeAddressKey = opencensus::tags::TagKey::Register("NodeAddress");

static opencensus::tags::TagKey VersionKey = opencensus::tags::TagKey::Register("Version");

static opencensus::tags::TagKey LanguageKey = opencensus::tags::TagKey::Register("Language");

static opencensus::tags::TagKey WorkerPidKey = opencensus::tags::TagKey::Register("WorkerPid");

/// The definitions of global tag.
static std::vector<std::pair<opencensus::tags::TagKey, std::string>> GlobalTags = {
    {JobNameKey, "raylet"},
    {VersionKey, "0.6.5"}
};

#endif // RAY_STATS_TAG_DEFS_H_
