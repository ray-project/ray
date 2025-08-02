//
// Created by jackt on 2025/7/16.
//

#include <iostream>
#include <string>
#include <cstring>
#include "ray/gcs/redis_cluster_key_locator.h"

#include "src/ray/util/logging.h"

extern "C" {
#include "hiredis/hiredis.h"
}
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"

namespace ray {
namespace gcs {

    RedisClusterKeyLocator::RedisClusterKeyLocator(const std::string& cluster_nodes)
        : seed_nodes_(parseClusterNodes(cluster_nodes)) {
        shuffleSeedNodes();
    }
    
    RedisClusterKeyLocator::~RedisClusterKeyLocator() {
        disconnect();
    }

    bool RedisClusterKeyLocator::isMultiShardRedisCluster() {
        // 尝试连接集群
        if (!connectToCluster()) {
            RAY_LOG(ERROR) << "Failed to connect to any cluster node";
            return false;
        }

        // 发送 CLUSTER INFO 命令获取集群信息
        redisReply* reply = reinterpret_cast<redisReply*>(
            redisCommand(redis_context_, "CLUSTER INFO")
        );

        // 检查命令执行错误
        if (reply == nullptr) {
            RAY_LOG(ERROR) << "Redis command failed: " << redis_context_->errstr;
            return false;
        }
        int cluster_size = 0;
        bool cluster_mode = false;
        std::vector<std::string> parts;
        switch (reply->type) {
            case REDIS_REPLY_STRING: {
                // 检查集群状态关键词
                parts = absl::StrSplit(reply->str, "\r\n");
                // Check the cluster status first
                for (const auto &part : parts) {
                    if (part.empty() || part[0] == '#') {
                        // it's a comment
                        continue;
                    }
                    std::vector<std::string> kv = absl::StrSplit(part, ":");
                    if (kv[0] == "cluster_state") {
                        if (kv[1] == "ok") {
                            cluster_mode = true;
                        } else if (kv[1] == "fail") {
                            RAY_LOG(FATAL)
                                << "The Redis cluster is not healthy. cluster_state shows failed status: "
                                << reply->str << "."
                                << " Please check Redis cluster used.";
                        }
                    }
                    if (kv[0] == "cluster_size") {
                        cluster_size = std::stoi(kv[1]);
                    }
                }
            }
            break;
            default:
                RAY_LOG(WARNING) << "Unexpected reply type for CLUSTER INFO: " << reply->type;
        }
        freeReplyObject(reply);
        RAY_LOG(INFO) << "Redis cluster detection : "
                      << (cluster_mode ? "CLUSTER" : "STANDALONE")
                      << (cluster_mode ? absl::StrCat(", shard size : ", cluster_size) : "");
        return cluster_mode && cluster_size > 1;
    }

    bool RedisClusterKeyLocator::isRedisSentinel() {
        // 尝试连接集群
        if (!connectToCluster()) {
            RAY_LOG(ERROR) << "Failed to connect to any cluster node";
            return false;
        }
        // 发送 INFO SENTINEL 命令获取哨兵信息
        redisReply* reply = reinterpret_cast<redisReply*>(
            redisCommand(redis_context_, "INFO SENTINEL")
        );

        // 检查命令执行错误
        if (reply == nullptr) {
            RAY_LOG(ERROR) << "Redis command failed: " << redis_context_->errstr;
            return false;
        }

        // 处理错误类型回复
        if (reply->type == REDIS_REPLY_ERROR) {
            RAY_LOG(ERROR) << "Redis returned error: " << reply->str;
            freeReplyObject(reply);
            return false;
        }

        // 处理字符串类型回复（有效响应）
        if (reply->type == REDIS_REPLY_STRING) {
            const bool is_sentinel = (reply->len > 0);
            freeReplyObject(reply);
            RAY_LOG(INFO) << "Detected Redis Sentinel: " << std::boolalpha << is_sentinel ;
            return is_sentinel;
        }

        // 处理其他意外响应类型
        RAY_LOG(ERROR) << "Unexpected reply type: " << reply->type;
        freeReplyObject(reply);
        return false;
    }

    // 查找key所属的节点信息
    bool RedisClusterKeyLocator::locateKeyNode(const std::string& key, std::string& node_host, int& node_port) {
        // 尝试连接集群
        if (!connectToCluster()) {
            RAY_LOG(ERROR) << "Failed to connect to any cluster node";
            return false;
        }
        // 1. 计算key对应的slot
        int slot = calculateKeySlot(key);
        if (slot < 0) return false;

        // 2. 获取集群槽分布
        if (!updateSlotMap()) {
            return false;
        }

        // 3. 查找slot对应的节点
        auto it = slot_map_.find(slot);
        if (it == slot_map_.end()) {
            RAY_LOG(ERROR) << "ERROR: Can't to find redis cluster slot owner: " << slot;
            return false;
        }

        node_host = it->second.first;
        node_port = it->second.second;
        return true;
    }

}  // namespace gcs
}  // namespace ray
