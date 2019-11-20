#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <string>
#include <thread>

#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/scheduling/scheduling_alg.h"
using namespace std;

/// Used to path empty vector argiuments.
vector<int64_t> EmptyIntVector;
vector<bool> EmptyBoolVector;

void initTaskRequest(TaskRequest &tr,
  vector<int64_t>& pred_demands, vector<bool>& pred_soft,
  vector<int64_t>& cust_ids, vector<int64_t>& cust_demands, vector<bool>& cust_soft,
  vector<int64_t>& placement_hints) {

  for (int i = 0; i < pred_demands.size(); i++) {
    ResourceRequest rq;
    rq.demand = pred_demands[i];
    rq.soft = pred_soft[i];
    tr.predefined_resources.push_back(rq);
  }

  for (int i = pred_demands.size(); i < PredefinedResources_MAX; i++) {
    ResourceRequest rq;
    rq.demand = 0;
    rq.soft = 0;
    tr.predefined_resources.push_back(rq);
  }

  for (int i = 0; i < cust_ids.size(); i++) {
    ResourceRequestWithId rq;
    rq.id = cust_ids[i];
    rq.req.demand = cust_demands[i];
    rq.req.soft = cust_soft[i];
    tr.custom_resources.push_back(rq);
  }

  for (int i = 0; i < placement_hints.size(); i++) {
    tr.placement_hints.insert(placement_hints[i]);
  }
};

void initNodeResources(NodeResources &node,
  vector<int64_t>& pred_capacities, vector<int64_t>& cust_ids,
  vector<int64_t>& cust_capacities) {

  for (int i = 0; i < pred_capacities.size(); i++) {
    ResourceCapacity rc;
    rc.total = rc.available = pred_capacities[i];
    node.capacities.push_back(rc);
  }

  if (pred_capacities.size() < PredefinedResources_MAX) {
    for (int i = pred_capacities.size(); i < PredefinedResources_MAX; i++) {
      ResourceCapacity rc;
      rc.total = rc.available = 0;
      node.capacities.push_back(rc);
    }
  }

  ResourceCapacity rc;
  for (int i = 0; i < cust_capacities.size(); i++) {
    rc.total = rc.available = cust_capacities[i];
    node.custom_resources.insert(pair<int64_t, ResourceCapacity>(cust_ids[i], rc));
  }
}

void initCluster(ClusterResourceScheduler &cluster_resources, int n)
{

  vector<int64_t> pred_capacities;
  vector<int64_t> cust_ids;
  vector<int64_t> cust_capacities;
  int i, k;

  for (i = 0; i < n; i++) {
    NodeResources node_resources;

    for (k = 0; k < PredefinedResources_MAX; k++) {
      if (rand() % 3 == 0) {
        pred_capacities.push_back(0);
      } else {
        pred_capacities.push_back(rand() % 10);
      }
    }

    int m = min(rand() % PredefinedResources_MAX, n);

    int start = rand() % n;
    for (k = 0; k < m; k++) {
      cust_ids.push_back((start + k) % n);
      cust_capacities.push_back(rand() % 10);
    }

    initNodeResources(node_resources, pred_capacities,
                      cust_ids, cust_capacities);

    cluster_resources.AddOrUpdateNode(i, node_resources);

    node_resources.custom_resources.clear();
  }
}

bool nodeResourcesEqual(NodeResources &nr1, NodeResources& nr2) {

  if (nr1.capacities.size() != nr2.capacities.size()) {
    cout << nr1.capacities.size() << " " << nr2.capacities.size() << endl;
    return false;
  }

  for (int i = 0; i < nr1.capacities.size(); i++) {
    if (nr1.capacities[i].available != nr2.capacities[i].available) {
      return false;
    }
    if (nr1.capacities[i].total != nr2.capacities[i].total) {
      return false;
    }
  }

  if (nr1.custom_resources.size() != nr2.custom_resources.size()) {
    return false;
  }

  auto cr1 = nr1.custom_resources;
  auto cr2 = nr2.custom_resources;
  for (auto it1 = cr1.begin(); it1 != cr1.end(); ++it1) {
    auto it2 = cr2.find(it1->first);
    if (it2 == cr2.end()) {
      return false;
    }
    if (it1->second.total != it2->second.total) {
      return false;
    }
    if (it1->second.available != it2->second.available) {
      return false;
    }
  }
  return true;
}

namespace ray {

class SchedulingTest : public ::testing::Test {
 public:
  void SetUp() {}

  void Shutdown() {}
};

TEST_F(SchedulingTest, SchedulingIdTest) {
  StringIdMap ids;
  hash<string> hasher;
  int num = 10;  // should be greater than 10.

  for (int i = 0; i < num; i++) {
    ids.Insert(to_string(i));
  }
  ASSERT_EQ(ids.Count(), num);

  ids.Remove(to_string(1));
  ASSERT_EQ(ids.Count(), num - 1);

  ids.Remove(hasher(to_string(2)));
  ASSERT_EQ(ids.Count(), num - 2);

  ASSERT_TRUE(ids.Get(to_string(3)) == static_cast<int64_t>(hasher(to_string(3))));

  ASSERT_TRUE(ids.Get(to_string(100)) == -1);

  /// Test for handling collision.
  StringIdMap short_ids;
  for (int i = 0; i < MAX_ID_TEST; i++) {
    /// "true" reduces the range of IDs to [0..9]
    int64_t id = short_ids.Insert(to_string(i), true);
    ASSERT_TRUE(id < MAX_ID_TEST);
  }
  ASSERT_EQ(short_ids.Count(), MAX_ID_TEST);
}

TEST_F(SchedulingTest, SchedulingInitClusterTest) {
  int num_nodes = 10;
  ClusterResourceScheduler cluster_resources;

  initCluster(cluster_resources, num_nodes);

  ASSERT_EQ(cluster_resources.NumNodes(), num_nodes);
}

TEST_F(SchedulingTest, SchedulingDeleteClusterNodeTest) {
  int num_nodes = 4;
  int64_t remove_id = 2;

  ClusterResourceScheduler cluster_resources;

  initCluster(cluster_resources, num_nodes);
  cluster_resources.RemoveNode(remove_id);

  ASSERT_TRUE(num_nodes - 1 == cluster_resources.NumNodes());
}

TEST_F(SchedulingTest, SchedulingModifyClusterNodeTest) {
  int num_nodes = 4;
  int64_t update_id = 2;
  ClusterResourceScheduler cluster_resources;

  initCluster(cluster_resources, num_nodes);

  NodeResources node_resources;
  vector<int64_t> pred_capacities;
  vector<int64_t> cust_ids;
  vector<int64_t> cust_capacities;
  int k;

  for (k = 0; k < PredefinedResources_MAX; k++) {
    if (rand() % 3 == 0) {
      pred_capacities.push_back(0);
    } else {
      pred_capacities.push_back(rand() % 10);
    }
  }

  int m = min(rand() % PredefinedResources_MAX, num_nodes);

  int start = rand() % num_nodes;
  for (k = 0; k < m; k++) {
    cust_ids.push_back((start + k) % num_nodes);
    cust_capacities.push_back(rand() % 10);

    initNodeResources(node_resources, pred_capacities,
                      cust_ids, cust_capacities);
    cluster_resources.AddOrUpdateNode(update_id, node_resources);
  }
  ASSERT_TRUE(num_nodes == cluster_resources.NumNodes());
}

TEST_F(SchedulingTest, SchedulingUpdateAvailableResourcesTest) {
  /// Create cluster resources.
  NodeResources node_resources;
  vector<int64_t> pred_capacities {10, 5, 3};
  vector<int64_t> cust_ids {1, 2};
  vector<int64_t> cust_capacities {5, 5};
  initNodeResources(node_resources, pred_capacities,
                    cust_ids, cust_capacities);
  ClusterResourceScheduler cluster_resources(1, node_resources);

  {
    TaskRequest task_req;
#define PRED_CUSTOM_LEN 2
    vector<int64_t> pred_demands {7, 7};
    vector<bool> pred_soft {false, true};
    vector<int64_t> cust_ids {1, 2};
    vector<int64_t> cust_demands {3, 10};
    vector<bool> cust_soft {false, true};
    initTaskRequest(task_req, pred_demands, pred_soft,
      cust_ids, cust_demands, cust_soft, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);

    NodeResources *pnr1 = cluster_resources.GetNodeResources(node_id);
    ASSERT_TRUE(pnr1 != NULL);
    NodeResources nr1 = *pnr1;
    cluster_resources.SubtractNodeAvailableResources(node_id, task_req);
    NodeResources *pnr2 = cluster_resources.GetNodeResources(node_id);
    ASSERT_TRUE(pnr2 != NULL);

    for (int i = 0; i < PRED_CUSTOM_LEN; i++) {
      int64_t t = nr1.capacities[i].available - task_req.predefined_resources[i].demand;
      if (t < 0) t = 0;
      ASSERT_EQ(pnr2->capacities[i].available, t);
    }

    for (int i = 0; i < PRED_CUSTOM_LEN; i++) {
      auto it1 = nr1.custom_resources.find(task_req.custom_resources[i].id);
      if (it1 != nr1.custom_resources.end()) {
        auto it2 = pnr2->custom_resources.find(task_req.custom_resources[i].id);
        if (it2 != pnr2->custom_resources.end()) {
          int64_t t = it1->second.available - task_req.custom_resources[i].req.demand;
          if (t < 0) t = 0;
          ASSERT_EQ(it2->second.available, t);
        }
      }
    }
  }
}


TEST_F(SchedulingTest, SchedulingAddOrUpdateNodeTest) {
  ClusterResourceScheduler cluster_resources;
  NodeResources nr;
  int64_t node_id = 1;

  /// Add node.
  {
    NodeResources node_resources;
    vector<int64_t> pred_capacities {10, 5, 3};
    vector<int64_t> cust_ids {1, 2};
    vector<int64_t> cust_capacities {5, 5};
    initNodeResources(node_resources, pred_capacities,
                      cust_ids, cust_capacities);
    cluster_resources.AddOrUpdateNode(node_id, node_resources);
    nr = node_resources;
  }

  /// Check whether node resources were correctly added.
  NodeResources *pnr = cluster_resources.GetNodeResources(node_id);
  ASSERT_TRUE(nodeResourcesEqual(*pnr, nr));

  /// Update node.
  {
    NodeResources node_resources;
    vector<int64_t> pred_capacities {10, 10};
    vector<int64_t> cust_ids {2, 3};
    vector<int64_t> cust_capacities {6, 6};
    initNodeResources(node_resources, pred_capacities,
                      cust_ids, cust_capacities);
    cluster_resources.AddOrUpdateNode(node_id, node_resources);
    nr = node_resources;
  }
  /// Check whether node resources were correctly updated.
  pnr = cluster_resources.GetNodeResources(node_id);
  ASSERT_TRUE(nodeResourcesEqual(*pnr, nr));
}


TEST_F(SchedulingTest, SchedulingTaskRequestTest) {
  /// Create cluster resources containing local node.
  NodeResources node_resources;
  vector<int64_t> pred_capacities {5, 5};
  vector<int64_t> cust_ids {1};
  vector<int64_t> cust_capacities {10};
  initNodeResources(node_resources, pred_capacities,
                    cust_ids, cust_capacities);
  ClusterResourceScheduler cluster_resources(0, node_resources);

  {
    NodeResources node_resources;
    vector<int64_t> pred_capacities {10, 2, 3};
    vector<int64_t> cust_ids {1, 2};
    vector<int64_t> cust_capacities {5, 5};
    initNodeResources(node_resources, pred_capacities,
                      cust_ids, cust_capacities);
    cluster_resources.AddOrUpdateNode(1, node_resources);
  }
  /// Predefined resources, hard constraint violation
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands = {11};
    vector<bool> pred_soft = {false};
    initTaskRequest(task_req, pred_demands, pred_soft,
      EmptyIntVector, EmptyIntVector, EmptyBoolVector, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_EQ(node_id, -1);
  }
  /// Predefined resources, soft constraint violation
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands = {11};
    vector<bool> pred_soft = {true};
    initTaskRequest(task_req, pred_demands, pred_soft,
      EmptyIntVector, EmptyIntVector, EmptyBoolVector, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }

  /// Predefined resources, no constraint violation.
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands = {5};
    vector<bool> pred_soft = {false};
    initTaskRequest(task_req, pred_demands, pred_soft,
      EmptyIntVector, EmptyIntVector, EmptyBoolVector, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
  /// Custom resources, hard constraint violation.
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands {5, 2};
    vector<bool> pred_soft {false, true};
    vector<int64_t> cust_ids {1};
    vector<int64_t> cust_demands {11};
    vector<bool> cust_soft {false};
    initTaskRequest(task_req, pred_demands, pred_soft,
      cust_ids, cust_demands, cust_soft, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id == -1);
  }
  /// Custom resources, soft constraint violation.
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands {5, 2};
    vector<bool> pred_soft {false, true};
    vector<int64_t> cust_ids {1};
    vector<int64_t> cust_demands {11};
    vector<bool> cust_soft {true};
    initTaskRequest(task_req, pred_demands, pred_soft,
      cust_ids, cust_demands, cust_soft, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }
  /// Custom resources, no constraint violation.
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands {5, 2};
    vector<bool> pred_soft {false, true};
    vector<int64_t> cust_ids {1};
    vector<int64_t> cust_demands {5};
    vector<bool> cust_soft {false};
    initTaskRequest(task_req, pred_demands, pred_soft,
      cust_ids, cust_demands, cust_soft, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
  /// Custom resource missing, hard constraint violation.
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands {5, 2};
    vector<bool> pred_soft {false, true};
    vector<int64_t> cust_ids {100};
    vector<int64_t> cust_demands {5};
    vector<bool> cust_soft {false};
    initTaskRequest(task_req, pred_demands, pred_soft,
      cust_ids, cust_demands, cust_soft, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id == -1);
  }
  /// Custom resource missing, soft constraint violation.
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands {5, 2};
    vector<bool> pred_soft {false, true};
    vector<int64_t> cust_ids {100};
    vector<int64_t> cust_demands {5};
    vector<bool> cust_soft {true};
    initTaskRequest(task_req, pred_demands, pred_soft,
      cust_ids, cust_demands, cust_soft, EmptyIntVector);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }
  /// Placement_hints, soft constraint violation.
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands {5, 2};
    vector<bool> pred_soft {false, true};
    vector<int64_t> cust_ids {1};
    vector<int64_t> cust_demands {5};
    vector<bool> cust_soft {true};
    vector<int64_t> placement_hints {2, 3};
    initTaskRequest(task_req, pred_demands, pred_soft,
      cust_ids, cust_demands, cust_soft, placement_hints);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }
  /// Placement hints, no constraint violation.
  {
    TaskRequest task_req;
    vector<int64_t> pred_demands {5, 2};
    vector<bool> pred_soft {false, true};
    vector<int64_t> cust_ids {1};
    vector<int64_t> cust_demands {5};
    vector<bool> cust_soft {true};
    vector<int64_t> placement_hints {1, 2, 3};
    initTaskRequest(task_req, pred_demands, pred_soft,
      cust_ids, cust_demands, cust_soft, placement_hints);
    int64_t violations;
    int64_t node_id = cluster_resources.GetBestSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
