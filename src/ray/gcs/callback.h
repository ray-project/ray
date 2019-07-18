#ifndef RAY_GCS_CALLBACK_H
#define RAY_GCS_CALLBACK_H

#include <boost/optional/optional.hpp>
#include <vector>
#include "ray/common/status.h"

namespace ray {

namespace gcs {

/// This callback is used to notify when a write/subscribe to GCS completes.
/// \param status Status indicates whether the write/subscribe was successful.
using StatusCallback = std::function<void(Status status)>;

/// This callback is used to receive one item from GCS when a read completes.
/// \param status Status indicates whether the read was successful.
/// \param result The item returned by GCS. If the item to read doesn't exist,
/// this optional object is empty.
template <typename Data>
using OptionalItemCallback =
    std::function<void(Status status, boost::optional<Data> result)>;

/// This callback is used to receive multiple items from GCS when a read completes.
/// \param status Status indicates whether the read was successful.
/// \param result The items returned by GCS.
template <typename Data>
using MultiItemCallback =
    std::function<void(Status status, const std::vector<Data> &result)>;

/// This callback is used to receive notifications of the subscribed items in the GCS.
/// \param id The id of the item.
/// \param result The notification message.
template <typename ID, typename Data>
using SubscribeCallback = std::function<void(const ID &id, const Data &result)>;

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_CALLBACK_H
