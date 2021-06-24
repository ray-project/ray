#pragma once

#include <ray/api/common_types.h>
#include <boost/dll.hpp>
#include <memory>
#include <msgpack.hpp>
#include <string>
#include <unordered_map>

namespace ray {
namespace api {

class FunctionHelper {
 public:
  static FunctionHelper &GetInstance() {
    static FunctionHelper functionHelper;
    return functionHelper;
  }

  std::shared_ptr<boost::dll::shared_library> LoadDll(const std::string &lib_name);
  std::function<msgpack::sbuffer(
      const std::string &, const std::vector<msgpack::sbuffer> &, msgpack::sbuffer *)>
  GetEntryFunction(const std::string &lib_name);

 private:
  FunctionHelper() = default;
  ~FunctionHelper() = default;
  FunctionHelper(FunctionHelper const &) = delete;
  FunctionHelper(FunctionHelper &&) = delete;
  std::string OutputAllRemoteFunctionNames(const boost::dll::shared_library &lib);
  std::unordered_map<std::string, std::shared_ptr<boost::dll::shared_library>> libraries_;
  std::unordered_map<std::string,
                     std::function<msgpack::sbuffer(const std::string &,
                                                    const std::vector<msgpack::sbuffer> &,
                                                    msgpack::sbuffer *)>>
      funcs_;
};
}  // namespace api
}  // namespace ray