#ifndef RAY_GCS_SUBSCRIPTION_EXECUTOR_H
#define RAY_GCS_SUBSCRIPTION_EXECUTOR_H

#include <atomic>
#include <list>
#include <mutex>
#include "ray/gcs/callback.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

/// \class SubscriptionExecutor
/// SubscriptionExecutor class encapsulates the implementation details of
/// subscribe/unsubscribe to elements (e.g.: actors or tasks or objects or nodes).
/// Support subscribing to a specific element or subscribing to all elements.
template <typename ID, typename Data, typename Table>
class SubscriptionExecutor {
 public:
  SubscriptionExecutor(Table &table) : table_(table) {}

  ~SubscriptionExecutor() {}

  /// Subscribe to operations of all elements.
  /// Repeated subscription will return a failure.
  ///
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each update will be received. Else, only
  /// messages for the given client will be received.
  /// \param subscribe Callback that will be called each time when an element
  /// is registered or updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  Status AsyncSubscribeAll(const ClientID &client_id,
                           const SubscribeCallback<ID, Data> &subscribe,
                           const StatusCallback &done);

  /// Subscribe to operations of an element.
  /// Repeated subscription to an element will return a failure.
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
  /// Unsubscribing can only be called after the subscription request is completed.
  ///
  /// \param client_id The type of update to listen to. If this is nil, then a
  /// message for each update will be received. Else, only
  /// messages for the given client will be received.
  /// \param id The id of the element to be unsubscribed to.
  /// \param done Callback that will be called when cancel subscription is complete.
  /// \return Status
  Status AsyncUnsubscribe(const ClientID &client_id, const ID &id,
                          const StatusCallback &done);

 private:
  Table &table_;

  std::mutex mutex_;

  enum class RegistrationStatus : uint8_t {
    kNotRegistered,
    kRegistering,
    kRegistered,
  };

  /// Whether successfully registered subscription to GCS.
  RegistrationStatus registration_status_{RegistrationStatus::kNotRegistered};

  /// List of subscriptions before registration to GCS is done, these callbacks
  /// will be called when the registration to GCS finishes.
  std::list<StatusCallback> pending_subscriptions_;

  /// Subscribe Callback of all elements.
  SubscribeCallback<ID, Data> subscribe_all_callback_{nullptr};

  /// A mapping from element ID to subscription callback.
  typedef std::unordered_map<ID, SubscribeCallback<ID, Data>> IDToCallbackMap;
  IDToCallbackMap id_to_callback_map_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_SUBSCRIPTION_EXECUTOR_H
