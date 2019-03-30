#include "ray/stats/metric.h"

namespace ray {

namespace stats {

std::unique_ptr<TagsType> &GetGlobalTagsPtr() {
  static std::unique_ptr<TagsType> global_tags_ptr(nullptr);
  return global_tags_ptr;
}

static void RegisterAsView(opencensus::stats::ViewDescriptor view_descriptor,
                           const std::vector<opencensus::tags::TagKey> &keys) {
  // Register global keys.
  if (GetGlobalTagsPtr() != nullptr) {
    for (const auto &tag : *GetGlobalTagsPtr()) {
      view_descriptor = view_descriptor.add_column(tag.first);
    }
  }

  // Register custom keys.
  for (const auto &key : keys) {
    view_descriptor = view_descriptor.add_column(key);
  }

  opencensus::stats::View view(view_descriptor);
  view_descriptor.RegisterForExport();
}

void Metric::Record(double value, const TagsType &tags) {
  if (measure_ == nullptr) {
    measure_.reset(new opencensus::stats::Measure<double>(
        opencensus::stats::Measure<double>::Register(name_, description_, unit_)));
    RegisterView();
  }

  RAY_CHECK(nullptr != GetGlobalTagsPtr()) << "global tags is nullptr.";
  // Do record.
  TagsType combined_tags(tags);
  combined_tags.insert(std::end(combined_tags), std::begin(*GetGlobalTagsPtr()),
                       std::end(*GetGlobalTagsPtr()));
  opencensus::stats::Record({{*measure_, value}}, combined_tags);
}

void Gauge::RegisterView() {
  opencensus::stats::ViewDescriptor view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name(name_)
          .set_description(description_)
          .set_measure(name_)
          .set_aggregation(opencensus::stats::Aggregation::LastValue());
  RegisterAsView(view_descriptor, tag_keys_);
}

void Histogram::RegisterView() {
  opencensus::stats::ViewDescriptor view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name(name_)
          .set_description(description_)
          .set_measure(name_)
          .set_aggregation(opencensus::stats::Aggregation::Distribution(
              opencensus::stats::BucketBoundaries::Explicit(boundaries_)));

  RegisterAsView(view_descriptor, tag_keys_);
}

void Count::RegisterView() {
  opencensus::stats::ViewDescriptor view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name(name_)
          .set_description(description_)
          .set_measure(name_)
          .set_aggregation(opencensus::stats::Aggregation::Count());

  RegisterAsView(view_descriptor, tag_keys_);
}

void Sum::RegisterView() {
  opencensus::stats::ViewDescriptor view_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name(name_)
          .set_description(description_)
          .set_measure(name_)
          .set_aggregation(opencensus::stats::Aggregation::Count());

  RegisterAsView(view_descriptor, tag_keys_);
}

}  // namespace stats

}  // namespace ray
