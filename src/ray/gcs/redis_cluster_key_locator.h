// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <iostream>
#include <string>
#include <algorithm>
#include <unordered_map>
#include <vector>
#include <random>
#include <stdexcept>
#include <memory>
#include <cstdint>
#include "ray/util/logging.h"

extern "C" {
#include "hiredis/hiredis.h"
struct redisAsyncContext;
}

namespace ray {
namespace gcs {

class RedisClusterKeyLocator {
public:
  explicit RedisClusterKeyLocator(const std::string& cluster_nodes);

  ~RedisClusterKeyLocator();

  bool isRedisSentinel();

  bool isMultiShardRedisCluster();

  bool locateKeyNode(const std::string& key, std::string& node_host, int& node_port);

private:
    // 集群节点信息
    std::vector<std::pair<std::string, int>> seed_nodes_;
    redisContext* redis_context_ = nullptr;
    std::unordered_map<int, std::pair<std::string, int>> slot_map_;

    // 节点解析和处理
    std::vector<std::pair<std::string, int>> parseClusterNodes(const std::string& nodes_str) {
        std::vector<std::pair<std::string, int>> nodes;
        size_t start = 0, end = 0;
        
        while (end != std::string::npos) {
            end = nodes_str.find(',', start);
            std::string token = (end == std::string::npos) ? 
                nodes_str.substr(start) : nodes_str.substr(start, end - start);
            
            size_t colon_pos = token.find(':');
            if (colon_pos != std::string::npos) {
                try {
                    std::string host = token.substr(0, colon_pos);
                    int port = std::stoi(token.substr(colon_pos + 1));
                    nodes.push_back({host, port});
                } catch (...) {
                    RAY_LOG(ERROR) << "Invalid node format: " << token;
                }
            }
            start = (end == std::string::npos) ? end : end + 1;
        }
        
        if (nodes.empty()) {
            throw std::runtime_error("No valid nodes provided");
        }
        
        return nodes;
    }
    
    void shuffleSeedNodes() {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(seed_nodes_.begin(), seed_nodes_.end(), g);
    }

    bool connectToCluster() {
        if (redis_context_) return true;
        
        for (const auto& node : seed_nodes_) {
            const std::string& host = node.first;
            int port = node.second;
            
            struct timeval timeout = {1, 500000}; // 1.5秒超时
            redisContext* ctx = redisConnectWithTimeout(host.c_str(), port, timeout);
            
            if (ctx && !ctx->err) {
                redis_context_ = ctx;
                RAY_LOG(INFO) << "Connected to node: " << host << ":" << port;
                return true;
            }
            
            if (ctx) {
                RAY_LOG(ERROR) << "Connection error (" << host << ":" << port
                          << "): " << ctx->errstr;
                redisFree(ctx);
            } else {
                RAY_LOG(ERROR) << "Connection failed (" << host << ":" << port << ")";
            }
        }
        
        return false;
    }
    
    void disconnect() {
        if (redis_context_) {
            redisFree(redis_context_);
            redis_context_ = nullptr;
        }
    }

    // 计算key对应的slot (使用Redis的CRC16算法)
    int calculateKeySlot(const std::string& key) {
        // 查找并提取哈希标签 {} 中的内容
        size_t start = key.find('{');
        if (start != std::string::npos) {
            size_t end = key.find('}', start+1);
            if (end != std::string::npos && end > start+1) {
                std::string tag = key.substr(start+1, end - start - 1);
                return crc16(tag.c_str(), tag.size()) % 16384;
            }
        }

        // 没有哈希标签则使用整个key
        return crc16(key.c_str(), key.size()) % 16384;
    }

    // CRC16算法 (Redis集群使用)
    unsigned int crc16(const char *buf, size_t len) {
        unsigned int crc = 0;
        static const unsigned int crc16tab[256] = {
            0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
            0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
            0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
            0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
            0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
            0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
            0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
            0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
            0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
            0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
            0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
            0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
            0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
            0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
            0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
            0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
            0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
            0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
            0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
            0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
            0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
            0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
            0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
            0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
            0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
            0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
            0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
            0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
            0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
            0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
            0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
            0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
        };

        for (size_t i = 0; i < len; i++) {
            crc = ((crc << 8) ^ crc16tab[((crc >> 8) ^ (0xff & buf[i]))]) & 0xffff;
        }
        return crc;
    }

    // 获取并更新集群槽分布
    bool updateSlotMap() {
        if (!redis_context_) return false;

        slot_map_.clear();

        // 执行CLUSTER SLOTS命令
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "CLUSTER SLOTS");
        if (!reply) {
            RAY_LOG(ERROR) << "ERROR : can't execute `CLUSTER SLOTS`";
            return false;
        }

        if (reply->type == REDIS_REPLY_ERROR) {
            RAY_LOG(ERROR) << "CLUSTER SLOTS ERROR : " << reply->str;
            freeReplyObject(reply);
            return false;
        }

        if (reply->type != REDIS_REPLY_ARRAY) {
            RAY_LOG(ERROR) << "ERROR : Expected to array response";
            freeReplyObject(reply);
            return false;
        }

        // 解析集群槽分配信息
        for (size_t i = 0; i < reply->elements; i++) {
            redisReply* slot_info = reply->element[i];
            if (slot_info->type != REDIS_REPLY_ARRAY || slot_info->elements < 3) {
                continue; // 跳过无效条目
            }

            // 解析槽范围
            if (slot_info->element[0]->type != REDIS_REPLY_INTEGER ||
                slot_info->element[1]->type != REDIS_REPLY_INTEGER) {
                continue;
            }

            long start_slot = slot_info->element[0]->integer;
            long end_slot = slot_info->element[1]->integer;

            // 解析主节点信息
            redisReply* master_info = slot_info->element[2];
            if (master_info->type != REDIS_REPLY_ARRAY || master_info->elements < 2) {
                continue;
            }

            // 解析IP和端口
            std::string node_host;
            int node_port = 0;

            if (master_info->element[0]->type == REDIS_REPLY_STRING) {
                node_host.assign(master_info->element[0]->str, master_info->element[0]->len);
            }
            if (master_info->element[1]->type == REDIS_REPLY_INTEGER) {
                node_port = master_info->element[1]->integer;
            }

            if (node_host.empty() || node_port == 0) {
                continue;
            }

            // 填充槽映射
            for (long slot = start_slot; slot <= end_slot; slot++) {
                slot_map_[slot] = std::make_pair(node_host, node_port);
            }
        }

        freeReplyObject(reply);
        return !slot_map_.empty();
    }
};

}  // namespace gcs
}  // namespace ray
