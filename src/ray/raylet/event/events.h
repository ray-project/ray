#ifndef RAY_EVENT_EVENTS_H
#define RAY_EVENT_EVENTS_H

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/id.h"

namespace ray {

namespace events {

/// The enum of event status.
enum class EventStatus { pending, cancelled, finished };

struct EventEdge;
template <typename TEvent>
class EventsManager;

class IEventNode {
 public:
  /// Finish an input edge of the node.
  ///
  /// \param in_edge The input edge.
  /// \return Void.
  virtual void FinishInEdge(std::shared_ptr<EventEdge> &in_edge) = 0;
  virtual void FinishInEdge(std::shared_ptr<EventEdge> &&in_edge) = 0;

  /// Finish the output edge of the node.
  ///
  /// \param out_edge The output edge.
  /// \return Void.
  virtual void FinishOutEdge(std::shared_ptr<EventEdge> &out_edge) = 0;
  virtual void FinishOutEdge(std::shared_ptr<EventEdge> &&out_edge) = 0;

  /// Cancel an input edge of the node.
  ///
  /// \param in_edge The input edge.
  /// \return Void.
  virtual void CancelInEdge(std::shared_ptr<EventEdge> &in_edge) = 0;
  virtual void CancelInEdge(std::shared_ptr<EventEdge> &&in_edge) = 0;

  /// Cancel the output edge of the node.
  ///
  /// \param out_edge The output edge.
  /// \return Void.
  virtual void CancelOutEdge(std::shared_ptr<EventEdge> &out_edge) = 0;
  virtual void CancelOutEdge(std::shared_ptr<EventEdge> &&out_edge) = 0;

  /// Finish the node.
  ///
  /// \return Void.
  virtual void Finish() = 0;

  /// Cancel the node.
  ///
  /// \return Void.
  virtual void Cancel() = 0;
};

struct EventEdge : std::enable_shared_from_this<EventEdge> {
  /// Cancel the edge.
  ///
  /// \return Void.
  virtual void Cancel() {
    if (status == EventStatus::pending) {
      status = EventStatus::cancelled;
      if (in_event_node) in_event_node->CancelOutEdge(shared_from_this());
      if (out_event_node) out_event_node->CancelInEdge(shared_from_this());
    }
  }

  /// Finish the edge.
  ///
  /// \return Void.
  virtual void Finish() {
    if (status == EventStatus::pending) {
      status = EventStatus::finished;
      if (in_event_node) in_event_node->FinishOutEdge(shared_from_this());
      if (out_event_node) out_event_node->FinishInEdge(shared_from_this());
    }
  }
  virtual ~EventEdge(){};
  /// Current status of the edge.
  EventStatus status = EventStatus::pending;
  /// The upstrean node. This edge serves as one of its output edges.
  std::shared_ptr<IEventNode> in_event_node;
  /// The downstrean node. This edge serves as one of its input edges.
  std::shared_ptr<IEventNode> out_event_node;
};

template <typename TEvent>
class EventNode : public IEventNode {
 public:
  /// Create an event node.
  ///
  /// \param event The event this node represents.
  /// \param manager The event manager this node belongs to.
  EventNode(const TEvent &event, EventsManager<TEvent> &manager)
      : event_(event), manager_(manager) {}
  virtual ~EventNode() {}

  void InsertOutEdge(std::shared_ptr<EventEdge> &out_node) {
    out_edges_.insert(out_node);
  }

  virtual void FinishInEdge(std::shared_ptr<EventEdge> &in_edge) override {
    auto extracted_in_edge = ExtractInEdge(in_edge);
    if (extracted_in_edge) extracted_in_edge->Finish();
    if (in_edges_.empty()) Finish();
  }

  virtual void FinishInEdge(std::shared_ptr<EventEdge> &&in_edge) override {
    FinishInEdge(in_edge);
  }

  virtual void FinishOutEdge(std::shared_ptr<EventEdge> &out_edge) override {
    auto extracted_out_edge = ExtractOutEdge(out_edge);
    if (extracted_out_edge) extracted_out_edge->Finish();
    if (out_edges_.empty()) Cancel();
  }

  virtual void FinishOutEdge(std::shared_ptr<EventEdge> &&out_edge) override {
    FinishOutEdge(out_edge);
  }

  virtual void CancelInEdge(std::shared_ptr<EventEdge> &in_edge) override {
    auto extracted_in_edge = ExtractInEdge(in_edge);
    if (extracted_in_edge) extracted_in_edge->Cancel();
    if (in_edges_.empty()) Finish();
  }

  virtual void CancelInEdge(std::shared_ptr<EventEdge> &&in_edge) override {
    CancelInEdge(in_edge);
  }

  virtual void CancelOutEdge(std::shared_ptr<EventEdge> &out_edge) override {
    auto extracted_out_edge = ExtractOutEdge(out_edge);
    if (extracted_out_edge) extracted_out_edge->Cancel();
    if (out_edges_.empty()) Cancel();
  }

  virtual void CancelOutEdge(std::shared_ptr<EventEdge> &&out_edge) override {
    CancelOutEdge(out_edge);
  }

  virtual void Cancel() override {
    if (status_ != EventStatus::pending) return;
    status_ = EventStatus::cancelled;
    if (!in_edges_.empty()) {
      std::vector<std::shared_ptr<EventEdge>> in_edges_copy(in_edges_.begin(),
                                                            in_edges_.end());
      // Cancel unfinished dependencies.
      for (auto &in_edge : in_edges_copy) CancelInEdge(in_edge);
    }
    if (!out_edges_.empty()) {
      std::vector<std::shared_ptr<EventEdge>> out_edges_copy(out_edges_.begin(),
                                                             out_edges_.end());
      // Cancel unfinished dependencies.
      for (auto &out_edge : out_edges_copy) CancelOutEdge(out_edge);
    }
    manager_.CancelEvent(event_);
  }

  virtual void Finish() override {
    if (status_ != EventStatus::pending) return;
    status_ = EventStatus::finished;
    if (!in_edges_.empty()) {
      std::vector<std::shared_ptr<EventEdge>> in_edges_copy(in_edges_.begin(),
                                                            in_edges_.end());
      // Cancel unfinished dependencies.
      for (auto &in_edge : in_edges_copy) CancelInEdge(in_edge);
    }
    std::vector<std::shared_ptr<EventEdge>> out_edges_copy(out_edges_.begin(),
                                                           out_edges_.end());
    for (auto &out_edge : out_edges_copy) FinishOutEdge(out_edge);
    manager_.FinishEvent(event_);
  }

 private:
  /// Remove and return an input edge if it exists; otherwise return nullptr.
  ///
  /// \param in_edge The input edge.
  /// \return The input edge.
  std::shared_ptr<EventEdge> ExtractInEdge(std::shared_ptr<EventEdge> &in_edge) {
    // We can use `extract` since C++14.
    auto search = in_edges_.find(in_edge);
    if (search != in_edges_.end()) {
      in_edges_.erase(in_edge);
      in_edge->out_event_node.reset();
      return in_edge;
    } else {
      return nullptr;
    }
  }

  /// Remove and return an output edge if it exists; otherwise return nullptr.
  ///
  /// \param out_edge The output edge.
  /// \return The output edge.
  std::shared_ptr<EventEdge> ExtractOutEdge(std::shared_ptr<EventEdge> &out_edge) {
    // We can use `extract` since C++14.
    auto search = out_edges_.find(out_edge);
    if (search != out_edges_.end()) {
      out_edges_.erase(out_edge);
      out_edge->in_event_node.reset();
      return out_edge;
    } else {
      return nullptr;
    }
  }

  /// The event this event node represents.
  const TEvent event_;
  /// The event manager of the event node.
  EventsManager<TEvent> &manager_;
  /// Current status of the event node.
  EventStatus status_ = EventStatus::pending;
  /// `in_edges_` are dependencies of some upstream events.
  std::unordered_set<std::shared_ptr<EventEdge>> in_edges_;
  /// `out_edges_` are dependencies of some downstream events.
  std::unordered_set<std::shared_ptr<EventEdge>> out_edges_;
};

/// An events manager maintains its event nodes.
template <typename TEvent>
class EventsManager {
 public:
  /// Subscribe an event. If the corresponding node does not exists, it will create one.
  ///
  /// \param event The event to subscribe.
  /// \param edge The output edge of the corresponding event node.
  /// \return Void
  void SubscribeEvent(const TEvent &event, std::shared_ptr<EventEdge> &edge) {
    auto event_node = CreateEventNode(event);
    event_node->InsertOutEdge(edge);
  }

  /// Cancel an event. If the event does not exists, it will be ignored.
  /// The corresponding node will be cancelled and erased from the event manager.
  ///
  /// \param event The event to cancel.
  /// \return Void
  void CancelEvent(const TEvent &event) {
    auto search = events_.find(event);
    if (search != events_.end()) {
      search->second->Cancel();
      events_.erase(event);
    }
  }

  /// Finish an event. If the event does not exists, it will be ignored.
  /// The corresponding node will be finished and erased from the event manager.
  ///
  /// \param event The event to finish.
  /// \return Void
  void FinishEvent(const TEvent &event) {
    auto search = events_.find(event);
    if (search != events_.end()) {
      search->second->Finish();
      events_.erase(event);
    }
  }

  virtual ~EventsManager() {}

 private:
  /// Try to create an event node with the given event.
  /// Return the existing one if the event node already exists.
  ///
  /// \param event The event for the event node.
  /// \return The corresponding event node.
  std::shared_ptr<EventNode<TEvent>> CreateEventNode(const TEvent &event) {
    auto search = events_.find(event);
    std::shared_ptr<EventNode<TEvent>> event_node;
    if (search == events_.end()) {
      event_node = std::make_shared<EventNode<TEvent>>(event, *this);
      InitializeEventNode(event_node);
      events_[event] = event_node;
    } else {
      event_node = search->second;
    }
    return event_node;
  }

  /// Initializer for a new event node.
  ///
  /// \param event_node The event node to be initialized.
  /// \return Void
  virtual void InitializeEventNode(std::shared_ptr<EventNode<TEvent>> &event_node) = 0;
  /// A mapping between events and event nodes.
  std::unordered_map<TEvent, std::shared_ptr<EventNode<TEvent>>> events_;
};

};  // namespace events
};  // namespace ray

#endif  // RAY_EVENT_EVENTS_H
