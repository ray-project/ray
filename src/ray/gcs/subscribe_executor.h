#ifndef RAY_GCS_SUBSCRIBE_EXECUTOR_H
#define RAY_GCS_SUBSCRIBE_EXECUTOR_H

#include <atomic>
#include <mutex>
#include "ray/gcs/callback.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

/// \class SubscribeExecutor
/// SubscribeExecutor class encapsulates the implementation details of
/// subscribe or unsubscribe to any operations of elements (actors or tasks or objects).
/// Either subscribe to specific elements or subscribe to all.
template <typename ID, typename Data, typename Table>
class SubscribeExecutor {
 public:
  SubscribeExecutor(Table &table) : table_(table) {}

  ~SubscribeExecutor() {}

  /// Subscribe to operations of all elements.
  ///
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each update will be received. Else, only
  /// messages for the given client will be received.
  /// \param subscribe Callback that will be called each time when an element
  /// is registered or updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribe(const ClientID &client_id,
                        const SubscribeCallback<ID, Data> &subscribe,
                        const StatusCallback &done);

  /// Subscribe to operations of an element.
  ///
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each update will be received. Else, only
  /// messages for the given client will be received.
  /// \param id The id of the element to be subscribe to.
  /// \param subscribe Callback that will be called each time when the element
  /// is registered or updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribe(const ClientID &client_id, const ID &id,
                        const SubscribeCallback<ID, Data> &subscribe,
                        const StatusCallback &done);

  /// Cancel subscription to an element.
  ///
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each update will be received. Else, only
  /// messages for the given client will be received.
  /// \param id The id of the element to be unsubscribed to.
  /// \param done Callback that will be called when cancel subscription is complete.
  /// \return Status
  Status AsyncUnsubscribe(const ClientID &client_id,
                          const ID &id, const StatusCallback &done);

 private:
  struct SubscribeCallbacks {
    SubscribeCallbacks() {}

    SubscribeCallbacks(const SubscribeCallback<ID, Data> &subscribe,
                       const StatusCallback &done)
        : subscribe_(subscribe), done_(done) {}

    SubscribeCallback<ID, Data> subscribe_{nullptr};
    StatusCallback done_{nullptr};
  };

 private:
  Table &table_;

  std::mutex mutex_;

  /// Whether successfully registered subscription to GCS.
  bool registered_{false};

  /// A mapping from element ID to subscription callbacks.
  typedef std::unordered_map<ID, SubscribeCallbacks> IDToRequestMap;
  IDToRequestMap id_to_request_map_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_SUBSCRIBE_EXECUTOR_H
