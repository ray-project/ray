#pragma once

#include <ray/api/common_types.h>
#include <ray/api/function_manager.h>
#include <boost/dll.hpp>
#include <memory>
#include <msgpack.hpp>
#include <string>
#include <unordered_map>

using namespace ::ray::internal;

namespace ray {
namespace api {

using EntryFuntion = std::function<msgpack::sbuffer(const void *, const std::string &,
                                                    const std::vector<msgpack::sbuffer> &,
                                                    msgpack::sbuffer *)>;

class FunctionHelper {
 public:
  static FunctionHelper &GetInstance() {
    static FunctionHelper functionHelper;
    return functionHelper;
  }

  void LoadDll(const boost::filesystem::path &lib_path);
  void LoadFunctionsFromPaths(const std::list<std::string> paths);
  std::pair<EntryFuntion, const void *> GetExecutableFunctions(
      const std::string &function_name, bool is_member_function);

 private:
  FunctionHelper() = default;
  ~FunctionHelper() = default;
  FunctionHelper(FunctionHelper const &) = delete;
  FunctionHelper(FunctionHelper &&) = delete;
  std::string LoadAllRemoteFunctions(const std::string lib_path,
                                     const boost::dll::shared_library &lib);
  std::unordered_map<std::string, std::shared_ptr<boost::dll::shared_library>> libraries_;
  std::unordered_map<std::string, EntryFuntion> entry_funcs_;
  std::unordered_map<std::string, std::pair<const RemoteFunctionMap_t &,
                                            const RemoteMemberFunctionMap_t &>>
      remote_funcs_;
};
}  // namespace api
}  // namespace ray