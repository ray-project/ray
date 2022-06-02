

#pragma once
#include "nlohmann/json.hpp"

using json = nlohmann::json;

class Pip {
  public:
    std::vector<std::string> packages;
    bool pip_check = false;
    Pip() = default;
    Pip(std::vector<std::string> packages, bool pip_check) :
      packages(packages), pip_check(pip_check) {
    }
};

void to_json(json& j, const Pip& pip) {
    j = json{{"packages", pip.packages}, {"pip_check", pip.pip_check}};
};

void from_json(const json& j, Pip& pip) {
    j.at("packages").get_to(pip.packages);
    j.at("pip_check").get_to(pip.pip_check);
};
