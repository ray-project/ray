#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <string>
#include <thread>

#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/scheduling/scheduling_alg.h"
using namespace std;


void initTaskReq(TaskReq &tr,
  int64_t *pred_demands, bool *pred_soft, int pred_len,
  int64_t *cust_ids, int64_t *cust_demands, bool *cust_soft, int cust_len,
  int64_t *placement_hints, int hints_len) {

  for (int i = 0; i < pred_len; i++) {
    ResourceReq rq;
    rq.demand = pred_demands[i];
    rq.soft = pred_soft[i];
    tr.predefined_resources.push_back(rq);
  }

  for (int i = pred_len; i < NUM_PREDIFINED_RESOURCES; i++) {
    ResourceReq rq;
    rq.demand = 0;
    rq.soft = 0;
    tr.predefined_resources.push_back(rq);
  }

  for (int i = 0; i < cust_len; i++) {
    ResourceReqWithId rq;
    rq.id = cust_ids[i];
    rq.req.demand = cust_demands[i];
    rq.req.soft = cust_soft[i];
    tr.custom_resources.push_back(rq);
  }

  for (int i = 0; i < hints_len; i++) {
    tr.placement_hints.insert(placement_hints[i]);
  }
};


void initNodeResources(NodeResources &node,
  int64_t *pred_capacities, int pred_len,
  int64_t *cust_ids, int64_t *cust_capacities,
  int cust_len) {

  for (int i = 0; i < pred_len; i++) {
    ResourceCapacity rc;
    rc.total = rc.available = pred_capacities[i];
    node.capacities.push_back(rc);
  }

  if (pred_len < NUM_PREDIFINED_RESOURCES) {
    for (int i = pred_len; i < NUM_PREDIFINED_RESOURCES; i++) {
      ResourceCapacity rc;
      rc.total = rc.available = 0;
      node.capacities.push_back(rc);
    }
  }

  ResourceCapacity rc;
  for (int i = 0; i < cust_len; i++) {
    rc.total = rc.available = cust_capacities[i];
    node.custom_resources.insert(pair<int64_t, ResourceCapacity>(cust_ids[i], rc));
  }
}

void initCluster(ClusterResources &cluster_resources, int n)
{

  int64_t pred_capacities[NUM_PREDIFINED_RESOURCES];
  int64_t cust_ids[NUM_PREDIFINED_RESOURCES];
  int64_t cust_capacities[NUM_PREDIFINED_RESOURCES];
  int i, k;

  for (i = 0; i < n; i++) {
    NodeResources node_resources;

    for (k = 0; k < NUM_PREDIFINED_RESOURCES; k++) {
      if (rand() % 3 == 0) {
        pred_capacities[k] = 0;
      } else {
        pred_capacities[k] = rand() % 10;
      }
    }

    int m = min(rand() % NUM_PREDIFINED_RESOURCES, n);

    int start = rand() % n;
    for (k = 0; k < m; k++) {
      cust_ids[k] = (start + k) % n;
      cust_capacities[k] = rand() % 10;
    }

    initNodeResources(node_resources, pred_capacities, NUM_PREDIFINED_RESOURCES,
                            cust_ids, cust_capacities, m);

    cluster_resources.add(i, node_resources);

    node_resources.custom_resources.clear();
  }
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
    ids.insert(to_string(i));
  }
  ASSERT_EQ(ids.count(), num);

  ids.remove(to_string(1));
  ASSERT_EQ(ids.count(), num - 1);

  ids.remove(hasher(to_string(2)));
  ASSERT_EQ(ids.count(), num - 2);

  ASSERT_TRUE(ids.get(to_string(3)) == static_cast<int64_t>(hasher(to_string(3))));

  ASSERT_TRUE(ids.get(to_string(100)) == -1);

  /// Test for handling collision.
  StringIdMap short_ids;
  for (int i = 0; i < 10; i++) {
    /// "true" reduces the range of IDs to [0..9]
    int64_t id = short_ids.insert(to_string(i), true);
    ASSERT_TRUE(id < 10);
  }
  ASSERT_EQ(short_ids.count(), 10);
}

TEST_F(SchedulingTest, SchedulingInitClusterTest) {
  int num_nodes = 10;
  ClusterResources cluster_resources;

  initCluster(cluster_resources, num_nodes);

  ASSERT_EQ(cluster_resources.count(), num_nodes);
}

TEST_F(SchedulingTest, SchedulingDeleteClusterNodeTest) {
  int num_nodes = 4;
  int64_t remove_id = 2;

  ClusterResources cluster_resources;

  initCluster(cluster_resources, num_nodes);
  cluster_resources.remove(remove_id);

  ASSERT_TRUE(num_nodes - 1 == cluster_resources.count());
}

TEST_F(SchedulingTest, SchedulingModifyClusterNodeTest) {
  int num_nodes = 4;
  int64_t update_id = 2;
  ClusterResources cluster_resources;

  initCluster(cluster_resources, num_nodes);

  NodeResources node_resources;
  int64_t pred_capacities[NUM_PREDIFINED_RESOURCES];
  int64_t cust_ids[NUM_PREDIFINED_RESOURCES];
  int64_t cust_capacities[NUM_PREDIFINED_RESOURCES];
  int k;

  for (k = 0; k < NUM_PREDIFINED_RESOURCES; k++) {
    if (rand() % 3 == 0) {
      pred_capacities[k] = 0;
    } else {
      pred_capacities[k] = rand() % 10;
    }
  }

  int m = min(rand() % NUM_PREDIFINED_RESOURCES, num_nodes);

  int start = rand() % num_nodes;
  for (k = 0; k < m; k++) {
    cust_ids[k] = (start + k) % num_nodes;
    cust_capacities[k] = rand() % 10;

    initNodeResources(node_resources, pred_capacities, NUM_PREDIFINED_RESOURCES,
                           cust_ids, cust_capacities, m);
    cluster_resources.add(update_id, node_resources);
  }
  ASSERT_TRUE(num_nodes == cluster_resources.count());
}

TEST_F(SchedulingTest, SchedulingUpdateAvailableResourcesTest) {
  /// Create cluster resources.
  NodeResources node_resources;
  int64_t pred_capacities[3] = {10, 5, 3};
  int64_t cust_ids[2] = {1, 2};
  int64_t cust_capacities[2] = {5, 5};
  initNodeResources(node_resources, pred_capacities, 3,
                    cust_ids, cust_capacities, 2);
  ClusterResources cluster_resources(1, node_resources);

  {
    TaskReq task_req;
#define PRED_CUSTOM_LEN 2
    int64_t pred_demands[PRED_CUSTOM_LEN] = {7, 7};
    bool pred_soft[PRED_CUSTOM_LEN] = {false, true};
    int64_t cust_ids[PRED_CUSTOM_LEN] = {1, 2};
    int64_t cust_demands[PRED_CUSTOM_LEN] = {3, 10};
    bool cust_soft[PRED_CUSTOM_LEN] = {false, true};
    initTaskReq(task_req, pred_demands, pred_soft, 2,
                cust_ids, cust_demands, cust_soft, 2, NULL, 0);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);

    NodeResources *pnr1 = cluster_resources.getNodeResources(node_id);
    ASSERT_TRUE(pnr1 != NULL);
    NodeResources nr1 = *pnr1;
    cluster_resources.updateAvailableResources(node_id, task_req);
    NodeResources *pnr2 = cluster_resources.getNodeResources(node_id);
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

TEST_F(SchedulingTest, SchedulingTaskRequestTest) {
  /// Create cluster resources containing local node.
  NodeResources node_resources;
  int64_t pred_capacities[2] = {5, 5};
  int64_t cust_ids[1] = {1};
  int64_t cust_capacities[1] = {10};
  initNodeResources(node_resources, pred_capacities, 2,
                    cust_ids, cust_capacities, 1);
  ClusterResources cluster_resources(0, node_resources);

  {
    NodeResources node_resources;
    int64_t pred_capacities[3] = {10, 2, 3};
    int64_t cust_ids[2] = {1, 2};
    int64_t cust_capacities[2] = {5, 5};
    initNodeResources(node_resources, pred_capacities, 3,
                          cust_ids, cust_capacities, 2);
    cluster_resources.add(1, node_resources);
  }
  /// Predefined resources, hard constraint violation
  {
    TaskReq task_req;
    int64_t pred_demands[1] = {11};
    bool pred_soft[1] = {false};
    initTaskReq(task_req, pred_demands, pred_soft, 1,
                     NULL, NULL, NULL, 0,
                     NULL, 0);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_EQ(node_id, -1);
  }
  /// Predefined resources, soft constraint violation
  {
    TaskReq task_req;
    int64_t pred_demands[1] = {11};
    bool pred_soft[1] = {true};
    initTaskReq(task_req, pred_demands, pred_soft, 1,
                     NULL, NULL, NULL, 0,
                     NULL, 0);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }

  /// Predefined resources, no constraint violation.
  {
    TaskReq task_req;
    int64_t pred_demands[1] = {5};
    bool pred_soft[1] = {false};
    initTaskReq(task_req, pred_demands, pred_soft, 1,
                     NULL, NULL, NULL, 0,
                     NULL, 0);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
  /// Custom resources, hard constraint violation.
  {
    TaskReq task_req;
    int64_t pred_demands[2] = {5, 2};
    bool pred_soft[2] = {false, true};
    int64_t cust_ids[1] = {1};
    int64_t cust_demands[1] = {11};
    bool cust_soft[2] = {false};
    initTaskReq(task_req, pred_demands, pred_soft, 2,
                     cust_ids, cust_demands, cust_soft, 1,
                     NULL, 0);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id == -1);
  }
  /// Custom resources, soft constraint violation.
  {
    TaskReq task_req;
    int64_t pred_demands[2] = {5, 2};
    bool pred_soft[2] = {false, true};
    int64_t cust_ids[1] = {1};
    int64_t cust_demands[1] = {11};
    bool cust_soft[2] = {true};
    initTaskReq(task_req, pred_demands, pred_soft, 2,
                     cust_ids, cust_demands, cust_soft, 1,
                     NULL, 0);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }
  /// Custom resources, no constraint violation.
  {
    TaskReq task_req;
    int64_t pred_demands[2] = {5, 2};
    bool pred_soft[2] = {false, true};
    int64_t cust_ids[1] = {1};
    int64_t cust_demands[1] = {5};
    bool cust_soft[2] = {false};
    initTaskReq(task_req, pred_demands, pred_soft, 2,
                     cust_ids, cust_demands, cust_soft, 1,
                     NULL, 0);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
  /// Custom resource missing, hard constraint violation.
  {
    TaskReq task_req;
    int64_t pred_demands[2] = {5, 2};
    bool pred_soft[2] = {false, true};
    int64_t cust_ids[1] = {100};
    int64_t cust_demands[1] = {5};
    bool cust_soft[2] = {false};
    initTaskReq(task_req, pred_demands, pred_soft, 2,
                     cust_ids, cust_demands, cust_soft, 1,
                     NULL, 0);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id == -1);
  }
  /// Custom resource missing, soft constraint violation.
  {
    TaskReq task_req;
    int64_t pred_demands[2] = {5, 2};
    bool pred_soft[2] = {false, true};
    int64_t cust_ids[1] = {100};
    int64_t cust_demands[1] = {5};
    bool cust_soft[2] = {true};
    initTaskReq(task_req, pred_demands, pred_soft, 2,
                     cust_ids, cust_demands, cust_soft, 1,
                     NULL, 0);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }
  /// Placement_hints, soft constraint violation.
  {
    TaskReq task_req;
    int64_t pred_demands[2] = {5, 2};
    bool pred_soft[2] = {false, true};
    int64_t cust_ids[1] = {1};
    int64_t cust_demands[1] = {5};
    bool cust_soft[2] = {true};
    int64_t placement_hints[2] = {2, 3};
    initTaskReq(task_req, pred_demands, pred_soft, 2,
                     cust_ids, cust_demands, cust_soft, 1,
                     placement_hints, 2);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations > 0);
  }
  /// Placement hints, no constraint violation.
  {
    TaskReq task_req;
    int64_t pred_demands[2] = {5, 2};
    bool pred_soft[2] = {false, true};
    int64_t cust_ids[1] = {1};
    int64_t cust_demands[1] = {5};
    bool cust_soft[1] = {true};
    int64_t placement_hints[3] = {1, 2, 3};
    initTaskReq(task_req, pred_demands, pred_soft, 2,
                     cust_ids, cust_demands, cust_soft, 1,
                     placement_hints, 3);
    int64_t violations;
    int64_t node_id = cluster_resources.getSchedulableNode(task_req, &violations);
    ASSERT_TRUE(node_id != -1);
    ASSERT_TRUE(violations == 0);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
