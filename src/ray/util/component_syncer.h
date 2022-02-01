
namespace ray {
namespace syncing {

struct Reporter {
  virtual const std::string* Snapshot(bool delta = false) const = 0;
};

struct Receiver {
  virtual void Update(const std::string& message) = 0;
  virtual const std::string* Snapshot(bool delta = false) const = 0;
};

class ComponentSyncer {
 public:
  explicit ComponentSyncer();
  void Register(const std::string& component, const Reporter* reporter, Receiver* receiver);

 private:
  absl::flat_hash_map<std::string, std::pair<const Reporter*, Receiver*>> components_;

  absl::flat_hash_map<ray::rpc::Address, std::vector<std::string>> messages_;

  std::vector<Connections> connections_;

};

}
}
