import { KeyboardArrowDown, KeyboardArrowRight } from "@mui/icons-material";
import {
  Alert,
  Box,
  Chip,
  Collapse,
  Grid,
  IconButton,
  Paper,
  Skeleton,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { ResourceUsage } from "../../components/ResourceUsage";
import { StatusChip } from "../../components/StatusChip";
import { getVirtualClusters } from "../../service/virtual_cluster";
import { VirtualCluster } from "../../type/virtual_cluster";
import { MainNavPageInfo } from "../layout/mainNavContext";

type VirtualClusterTreeNode = VirtualCluster & {
  children: VirtualClusterTreeNode[];
};

const formatBytes = (bytes: number) => {
  if (bytes === 0) {
    return "0 Bytes";
  }

  const k = 1024;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(3)) + " " + sizes[i];
};

const buildClusterTree = (
  clusters: VirtualCluster[],
): VirtualClusterTreeNode | null => {
  const clusterMap = new Map(clusters.map((c) => [c.name, c]));
  const root = clusters.find((c) => c.name === "kPrimaryClusterID");
  if (!root) {
    return null;
  }

  const buildTree = (cluster: VirtualCluster): VirtualClusterTreeNode => {
    const node: VirtualClusterTreeNode = {
      ...cluster,
      children: Object.keys(cluster.dividedClusters || {})
        .map((id) => clusterMap.get(id))
        .filter((c): c is VirtualCluster => Boolean(c))
        .map(buildTree),
    };

    if (cluster.divisible) {
      const replicaSets: Record<string, number> = {};
      Object.values(cluster.undividedNodes || {}).forEach((node) => {
        if (node.template_id) {
          replicaSets[node.template_id] =
            (replicaSets[node.template_id] || 0) + 1;
        }
      });

      const totalResources: Record<string, number> = {};

      Object.values(cluster.undividedNodes || {}).forEach((node) => {
        const usage = node.resources_usage || {};

        Object.entries(usage).forEach(([resourceName, usageStr]) => {
          const usageValues = usageStr ? usageStr.split(" / ") : ["0", "0"];
          const totalUsageStr = usageValues[1] || "0";

          let totalUsage = 0;
          if (resourceName.toLowerCase().includes("memory")) {
            // Extract both number and unit for memory resources.
            const match = totalUsageStr.match(/([\d.]+)\s*([a-zA-Z]+)/);
            if (match) {
              const number = parseFloat(match[1]);
              const unit = match[2].toLowerCase();
              let multiplier = 1;
              if (unit === "kb" || unit === "kib") {
                multiplier = 1024;
              } else if (unit === "mb" || unit === "mib") {
                multiplier = 1024 * 1024;
              } else if (unit === "gb" || unit === "gib") {
                multiplier = 1024 * 1024 * 1024;
              } else if (unit === "tb" || unit === "tib") {
                multiplier = 1024 * 1024 * 1024 * 1024;
              }
              totalUsage = number * multiplier;
            } else {
              // Fallback: try parsing the numeric part even without a unit.
              totalUsage = parseFloat(totalUsageStr) || 0;
            }
          } else {
            // For non-memory resources, just extract the numeric value.
            const match = totalUsageStr.match(/[\d.]+/);
            totalUsage = match ? parseFloat(match[0]) : 0;
          }

          totalResources[resourceName] =
            (totalResources[resourceName] || 0) + totalUsage;
        });
      });

      const resourcesUsage: Record<string, string> = {};
      const resources: Record<string, string> = {};

      Object.entries(totalResources).forEach(([resourceName, total]) => {
        const formattedTotal = resourceName.toLowerCase().includes("memory")
          ? formatBytes(total)
          : total.toFixed(1);

        resourcesUsage[resourceName] = `0 / ${formattedTotal}`;
        resources[resourceName] = String(total);
      });

      node.children.unshift({
        name: "Undivided",
        divisible: true,
        resourcesUsage: resourcesUsage,
        replicaSets: replicaSets,
        undividedNodes: cluster.undividedNodes,
        dividedClusters: {},
        undividedReplicaSets: {},
        resources: {
          CPU: resources["CPU"] || "0",
          memory: resources["memory"] || "0",
          object_store_memory: resources["object_store_memory"] || "0",
        },
        children: [],
      } as VirtualClusterTreeNode);
    }

    return node;
  };

  return buildTree(root);
};

const filterClusterTree = (
  node: VirtualClusterTreeNode,
  query: string,
): VirtualClusterTreeNode | null => {
  // If there is no search query, return the entire node unmodified.
  if (!query.trim()) {
    return node;
  }

  const lowerQuery = query.toLowerCase();

  const filteredChildren = (node.children || [])
    .map((child) => filterClusterTree(child, query))
    .filter((child): child is VirtualClusterTreeNode => child !== null);

  const filteredUndividedNodes = Object.entries(
    node.undividedNodes || {},
  ).reduce<Record<string, any>>((filtered, [nodeId, nodeObj]) => {
    const nodeSearchString = `${nodeId} ${nodeObj.hostname} ${
      nodeObj.template_id || "Unknown"
    } ${nodeObj.is_dead ? "DEAD" : "ALIVE"}`.toLowerCase();
    if (nodeSearchString.includes(lowerQuery)) {
      filtered[nodeId] = nodeObj;
    }
    return filtered;
  }, {});

  // Build a text string from the parent's (cluster's) own properties (excluding its nodes)
  const parentText = (
    node.name +
    " " +
    (node.divisible ? "divisible" : "indivisible") +
    " " +
    JSON.stringify(node.replicaSets) +
    " " +
    JSON.stringify(node.undividedReplicaSets) +
    " " +
    JSON.stringify(node.resourcesUsage) +
    " " +
    JSON.stringify(node.resources)
  ).toLowerCase();

  const parentMatches = parentText.includes(lowerQuery);

  // If either the parent's own details, any children cluster, or any node matches, keep this cluster.
  if (
    parentMatches ||
    filteredChildren.length > 0 ||
    Object.keys(filteredUndividedNodes).length > 0
  ) {
    return {
      ...node,
      children: filteredChildren,
      undividedNodes: filteredUndividedNodes,
    };
  }
  return null;
};

const ResourceOverview = ({ cluster }: { cluster: VirtualClusterTreeNode }) => {
  const resourceOrder = ["CPU", "memory", "object_store_memory"];
  const usageEntries = Object.entries(cluster.resourcesUsage || {}).sort(
    ([a], [b]) => {
      const indexA = resourceOrder.indexOf(a);
      const indexB = resourceOrder.indexOf(b);
      if (indexA !== -1 && indexB !== -1) {
        return indexA - indexB;
      }
      if (indexA !== -1) {
        return -1;
      }
      if (indexB !== -1) {
        return 1;
      }
      return a.localeCompare(b);
    },
  );
  return (
    <Box
      sx={{
        display: "flex",
        flexWrap: "wrap",
        gap: 1,
        alignItems: "flex-start",
        paddingTop: "2px",
      }}
    >
      {usageEntries.map(([resourceName, usageStr]) => (
        <Box
          key={resourceName}
          sx={{ display: "flex", alignItems: "center", gap: 0.5 }}
        >
          <ResourceUsage
            resourceName={resourceName}
            usageStr={typeof usageStr === "string" ? usageStr : "0/0"}
          />
        </Box>
      ))}
    </Box>
  );
};

const NodeResourceOverview = ({ node }: { node: any }) => {
  const resourceOrder = ["CPU", "memory", "object_store_memory"];
  const usageEntries = Object.entries(node.resources_usage || {}).sort(
    ([a], [b]) => {
      const indexA = resourceOrder.indexOf(a);
      const indexB = resourceOrder.indexOf(b);
      if (indexA !== -1 && indexB !== -1) {
        return indexA - indexB;
      }
      if (indexA !== -1) {
        return -1;
      }
      if (indexB !== -1) {
        return 1;
      }
      return a.localeCompare(b);
    },
  );
  return usageEntries.length ? (
    <Box
      sx={{ display: "flex", flexWrap: "wrap", gap: 1, alignItems: "center" }}
    >
      {usageEntries.map(([resourceName, usageStr]) => (
        <Box
          key={resourceName}
          sx={{ display: "flex", alignItems: "center", gap: 0.5 }}
        >
          <ResourceUsage
            resourceName={resourceName}
            usageStr={typeof usageStr === "string" ? usageStr : "0/0"}
          />
        </Box>
      ))}
    </Box>
  ) : null;
};

const ClusterTreeNode = ({
  cluster,
  level = 0,
}: {
  cluster: VirtualClusterTreeNode;
  level?: number;
}) => {
  const [expanded, setExpanded] = useState(
    level === 0 || cluster.name === "kPrimaryClusterID",
  );
  const [nodesExpanded, setNodesExpanded] = useState(false);
  const hasChildren = cluster.children?.length > 0;
  const isPrimaryCluster = cluster.name === "kPrimaryClusterID";

  return (
    <Box sx={{ ml: level * 2 }}>
      <Paper
        elevation={0}
        sx={{
          p: 1.5,
          mb: 1,
          border: "1px solid",
          borderColor: "divider",
          "&:hover": { bgcolor: "action.hover" },
        }}
      >
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <Box
            sx={{
              display: "flex",
              alignItems: "flex-start",
              flexWrap: "nowrap",
              gap: 1,
            }}
          >
            <IconButton
              size="small"
              onClick={() =>
                hasChildren
                  ? setExpanded(!expanded)
                  : setNodesExpanded(!nodesExpanded)
              }
            >
              {(hasChildren ? expanded : nodesExpanded) ? (
                <KeyboardArrowDown />
              ) : (
                <KeyboardArrowRight />
              )}
            </IconButton>
            <Box
              sx={{
                display: "flex",
                alignItems: "flex-start",
                flexWrap: "nowrap",
                gap: 1,
              }}
            >
              <Chip
                label={cluster.name}
                color={isPrimaryCluster ? "primary" : "default"}
                size="small"
              />
              <Chip
                label={cluster.divisible ? "Divisible" : "Indivisible"}
                variant="outlined"
                size="small"
                color={cluster.divisible ? "info" : "default"}
              />
              {Object.entries(cluster.replicaSets || {})
                .sort(([aKey], [bKey]) => aKey.localeCompare(bKey))
                .map(([key, value]) => (
                  <Chip
                    key={key}
                    label={`${key}: ${value}`}
                    variant="outlined"
                    size="small"
                  />
                ))}
              <ResourceOverview cluster={cluster} />
            </Box>
          </Box>

          <Collapse in={nodesExpanded}>
            <Box sx={{ mt: 1 }}>
              <Typography
                variant="subtitle2"
                color="text.secondary"
                sx={{ mb: 1 }}
              >
                Nodes ({Object.keys(cluster.undividedNodes || {}).length})
              </Typography>
              <Grid container spacing={1}>
                {Object.entries(cluster.undividedNodes || {})
                  .sort(([, a], [, b]) => a.hostname.localeCompare(b.hostname))
                  .map(([nodeId, node]) => (
                    <Grid item xs={12} key={nodeId}>
                      <Paper
                        variant="outlined"
                        sx={{
                          p: 1,
                          "&:hover": { bgcolor: "action.hover" },
                        }}
                      >
                        <Box
                          sx={{
                            display: "flex",
                            alignItems: "center",
                            gap: 1,
                          }}
                        >
                          <StatusChip
                            type="node"
                            status={node.is_dead ? "DEAD" : "ALIVE"}
                          />
                          <Typography variant="body2" sx={{ fontWeight: 500 }}>
                            {node.hostname}
                          </Typography>
                          <Chip
                            size="small"
                            variant="outlined"
                            label={`Node Type: ${
                              node.template_id || "Unknown"
                            }`}
                          />
                          <Chip
                            size="small"
                            label={nodeId}
                            variant="outlined"
                            onClick={() =>
                              (window.location.href = `#/cluster/nodes/${nodeId}`)
                            }
                            sx={{
                              color: "rgb(3, 109, 207)",
                              textDecoration: "underline",
                              cursor: "pointer",
                              "& .MuiChip-label": {
                                userSelect: "text",
                                padding: "4px 8px",
                                textDecoration: "underline",
                                whiteSpace: "nowrap",
                                fontSize: "0.75rem",
                              },
                              "&:hover": {
                                color: "darkblue",
                                backgroundColor: "transparent",
                                textDecoration: "underline",
                              },
                            }}
                          />
                        </Box>
                        <Box sx={{ mt: 1 }}>
                          <NodeResourceOverview node={node} />
                        </Box>
                      </Paper>
                    </Grid>
                  ))}
              </Grid>
            </Box>
          </Collapse>

          <Collapse in={expanded}>
            <Box sx={{ mt: 1 }}>
              {cluster.children?.map((child) => (
                <ClusterTreeNode
                  key={child.name}
                  cluster={child}
                  level={level + 1}
                />
              ))}
            </Box>
          </Collapse>
        </Box>
      </Paper>
    </Box>
  );
};

export const VirtualClustersPage = () => {
  const [clusters, setClusters] = useState<VirtualCluster[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const [requestStatus, setRequestStatus] = useState(
    "Loading virtual clusters...",
  );
  const [isRefreshing, setIsRefreshing] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");

  const fetchClusters = async () => {
    try {
      const data = await getVirtualClusters();
      setClusters(data);
      setRequestStatus("Fetched virtual clusters");
    } catch (err) {
      setError(err instanceof Error ? err : new Error("Unknown error"));
      setRequestStatus("Error fetching virtual clusters");
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchClusters();
    let interval: NodeJS.Timeout | null = null;
    if (isRefreshing) {
      interval = setInterval(fetchClusters, 4000);
    }
    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [isRefreshing]);

  const fullTree = useMemo(() => buildClusterTree(clusters), [clusters]);

  const filteredTree = useMemo(() => {
    if (!fullTree) {
      return null;
    }
    if (!searchQuery.trim()) {
      return fullTree;
    }
    return filterClusterTree(fullTree, searchQuery);
  }, [fullTree, searchQuery]);

  return (
    <Box sx={{ padding: 2 }}>
      <MainNavPageInfo
        pageInfo={{
          title: "Virtual Clusters",
          id: "virtual-clusters",
          path: "/virtual-clusters",
        }}
      />
      <Paper
        elevation={0}
        sx={{ p: 2, mb: 2, border: "1px solid", borderColor: "divider" }}
      >
        <Typography variant="h6">Virtual Clusters</Typography>
        <Box sx={{ mt: 1 }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            <Typography>Auto Refresh:</Typography>
            <Switch
              checked={isRefreshing}
              onChange={(e) => setIsRefreshing(e.target.checked)}
            />
          </Box>
          <Typography sx={{ mt: 1 }}>
            Request Status: {requestStatus}
          </Typography>
        </Box>
      </Paper>
      <Paper
        elevation={0}
        sx={{ p: 2, mb: 2, border: "1px solid", borderColor: "divider" }}
      >
        <Typography variant="h6">Virtual Cluster List</Typography>
        <Box sx={{ mt: 2 }}>
          <TextField
            label="Search Virtual Clusters"
            variant="outlined"
            fullWidth
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </Box>
        <Box sx={{ mt: 4 }}>
          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error.message}
            </Alert>
          )}
          {isLoading ? (
            <Box sx={{ p: 2 }}>
              <Skeleton variant="rectangular" height={200} />
            </Box>
          ) : filteredTree ? (
            <ClusterTreeNode cluster={filteredTree} />
          ) : (
            <Typography variant="body1" color="text.secondary">
              No matching virtual clusters found
            </Typography>
          )}
        </Box>
      </Paper>
    </Box>
  );
};
