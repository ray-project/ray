#ifndef _RAY_TAG_DEFS_H_
#define _RAY_TAG_DEFS_H_
// TODO(qwang): Add docstring and namespace declaration here.

static opencensus::tags::TagKey JobNameKey = opencensus::tags::TagKey::Register("JobName");

static opencensus::tags::TagKey NodeAddressKey = opencensus::tags::TagKey::Register("NodeAddress");

#endif // _RAY_TAG_DEFS_H_
