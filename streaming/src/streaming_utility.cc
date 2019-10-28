#include <boost/asio/ip/host_name.hpp>
#include <unordered_set>

#include "streaming_utility.h"
namespace ray {
namespace streaming {
std::string StreamingUtility::Byte2hex(const uint8_t *data, uint32_t data_size) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (uint32_t i = 0; i < data_size; i++) {
    unsigned short val = data[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

std::string StreamingUtility::Hexqid2str(const std::string &q_id_hex) {
  std::string result;
  for (uint32_t i = 0; i < q_id_hex.size(); i += 2) {
    std::string byte = q_id_hex.substr(i, 2);
    char chr = static_cast<char>(std::strtol(byte.c_str(), nullptr, 16));
    result.push_back(chr);
  }
  return result;
}

std::string StreamingUtility::GetHostname() { return boost::asio::ip::host_name(); }

void StreamingUtility::Split(const ray::ObjectID &q_id,
                             std::vector<std::string> &q_splited_vec) {
  for (uint32_t i = kUniqueIDSize - 4; i < kUniqueIDSize; i += 2) {
    q_splited_vec.push_back(std::to_string(q_id.Data()[i] * 0x100 + q_id.Data()[i + 1]));
  }
}

void StreamingUtility::FindTagsFromQueueName(const ray::ObjectID &q_id, TagMap &tags,
                                             bool is_reader) {
  std::string qname = StreamingUtility::Hexqid2str(q_id.Hex());
  std::vector<std::string> splited_vec;
  StreamingUtility::Split(q_id, splited_vec);

  if (splited_vec.size() < 2) {
    tags["qname"] = qname;
  } else {
    if (is_reader) {
      tags["worker_id"] = splited_vec[1];
    } else {
      tags["worker_id"] = splited_vec[0];
    }
    tags["qname"] = splited_vec[0] + "-" + splited_vec[1];
  }
}
std::string StreamingUtility::Qid2EdgeInfo(const ray::ObjectID &q_id) {
  std::vector<std::string> str_vec;
  Split(q_id, str_vec);
  STREAMING_CHECK(str_vec.size() == 2);
  return str_vec[0] + "-" + str_vec[1];
}

std::vector<ObjectID> StreamingUtility::SetDifference(
    const std::vector<ObjectID> &ids_a, const std::vector<ObjectID> &ids_b) {
  std::vector<ObjectID> diff_ids;
  std::unordered_set<ObjectID> id_b_set(ids_b.begin(), ids_b.end());
  std::copy_if(
      ids_a.begin(), ids_a.end(), std::back_inserter(diff_ids),
      [&id_b_set](const ObjectID &id) { return id_b_set.find(id) == id_b_set.end(); });
  return diff_ids;
}
}  // namespace streaming
}  // namespace ray
