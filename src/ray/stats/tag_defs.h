#ifndef _RAY_TAG_DEFS_H_
#define _RAY_TAG_DEFS_H_

static opencensus::tags::TagKey JobNameKey = opencensus::tags::TagKey::Register("JobName");

static opencensus::tags::TagKey CustomKey =  opencensus::tags::TagKey::Register("CustomKey");

static opencensus::tags::TagKey NodeAddressKey = opencensus::tags::TagKey::Register("NodeAddress");

static opencensus::tags::TagKey VersionKey = opencensus::tags::TagKey::Register("VersionKey");

/// Global tags definition.
static std::vector<std::pair<opencensus::tags::TagKey, std::string>> GlobalTags = {
    {JobNameKey, "raylet"},
    {VersionKey, "0.6.4"}
};

#endif // _RAY_TAG_DEFS_H_
