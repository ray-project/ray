import {
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  TextField,
} from "@mui/material";
import * as d3 from "d3";
import React, {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from "react";
import { PhysicalViewData } from "../../service/physical-view";

// Type for resource values
type ResourceValue = {
  total: number;
  available: number;
};

// Utility function to extract resource usage from node data
const extractResourceUsage = (
  resources: Record<string, ResourceValue>,
  pgId: string,
  resourceType: string,
  nodeData?: NodeData,
) => {
  // First try the existing resource extraction logic
  // Convert pgId to lowercase for case-insensitive matching
  const pgIdLower = pgId.toLowerCase();

  // Find the matching resource key
  const matchingKey = Object.keys(resources).find((key) => {
    if (!key.includes("Group")) {
      return false;
    }
    const [type, id] = key.split("Group");
    return (
      type.toLowerCase() === resourceType.toLowerCase() &&
      id.toLowerCase() === pgIdLower
    );
  });

  if (matchingKey) {
    // For GPU resources, calculate memory usage from gpuDevices
    if (resourceType.toLowerCase() === "gpu" && nodeData?.actors) {
      // Find all actors in this placement group
      let totalMemoryUsed = 0;
      let totalMemoryAvailable = 0;

      // Sum up memory usage from all actors in this placement group
      Object.values(nodeData.actors).forEach((actor: Actor) => {
        if (actor?.placementGroup?.id?.toLowerCase() === pgIdLower) {
          // Check if actor has resource_usage field for GPU
          const resourceUsage = getResourceUsageFromField(actor, resourceType);
          if (resourceUsage) {
            totalMemoryUsed += resourceUsage.used;
            totalMemoryAvailable += resourceUsage.total;
          } else {
            // Sum up memory from all GPU devices assigned to this actor
            actor.gpuDevices?.forEach((gpu) => {
              totalMemoryUsed += gpu.memoryUsed;
              totalMemoryAvailable += gpu.memoryTotal;
            });
          }
        }
      });

      // If we found any GPU memory usage
      if (totalMemoryAvailable > 0) {
        // Cap usage at 100%
        const usage = Math.min(totalMemoryUsed / totalMemoryAvailable, 1);

        return {
          available: Math.max(totalMemoryAvailable - totalMemoryUsed, 0),
          total: totalMemoryAvailable,
          used: totalMemoryUsed,
          usage: usage,
        };
      }
    }
    // For CPU resources, calculate CPU usage from processStats or nodeCpuPercent or resource_usage
    else if (resourceType.toLowerCase() === "cpu" && nodeData?.actors) {
      // Find all actors in this placement group
      let totalCpuPercent = 0;
      let actorCount = 0;
      let hasNodeCpuInfo = false;
      let hasResourceUsageInfo = false;
      let totalFromResourceUsage = 0;

      // Sum up CPU usage from all actors in this placement group
      Object.values(nodeData.actors).forEach((actor: Actor) => {
        if (actor?.placementGroup?.id?.toLowerCase() === pgIdLower) {
          // First check if actor has resource_usage field for CPU
          const resourceUsage = getResourceUsageFromField(actor, resourceType);
          if (resourceUsage) {
            totalCpuPercent += resourceUsage.used;
            totalFromResourceUsage += resourceUsage.total;
            hasResourceUsageInfo = true;
            actorCount++;
          }
          // If any actor has nodeCpuPercent, use that instead
          else if (
            actor.nodeCpuPercent !== undefined &&
            !hasResourceUsageInfo
          ) {
            totalCpuPercent = actor.nodeCpuPercent;
            hasNodeCpuInfo = true;
            return; // Exit the loop early once we find node CPU info
          }
          // Otherwise use processStats
          else if (
            !hasResourceUsageInfo &&
            actor.processStats &&
            actor.processStats.cpuPercent !== undefined
          ) {
            totalCpuPercent += actor.processStats.cpuPercent;
            actorCount++;
          }
        }
      });

      // If we found any CPU usage
      if (hasResourceUsageInfo || hasNodeCpuInfo || actorCount > 0) {
        // Cap at 100% for visualization purposes if not using resource_usage
        if (!hasResourceUsageInfo) {
          const cappedUsage = Math.min(totalCpuPercent, 100);
          return {
            available: 100 - cappedUsage,
            total: 100,
            used: totalCpuPercent, // Keep original value for display
            usage: cappedUsage / 100,
          };
        } else {
          // Use values from resource_usage and cap at 100%
          const usage = Math.min(totalCpuPercent / totalFromResourceUsage, 1);
          return {
            available: Math.max(totalFromResourceUsage - totalCpuPercent, 0),
            total: totalFromResourceUsage,
            used: totalCpuPercent,
            usage: usage,
          };
        }
      }
    }
    // For Memory resources, calculate memory usage from processStats or resource_usage
    else if (resourceType.toLowerCase() === "memory" && nodeData?.actors) {
      // Find all actors in this placement group
      let totalMemoryUsed = 0;
      let memoryTotal = 0;
      let memoryAvailable = 0;
      let hasNodeMemInfo = false;
      let actorCount = 0;

      // First try to get node memory info from any actor
      Object.values(nodeData.actors).forEach((actor: Actor) => {
        if (actor.nodeMem && actor.nodeMem.length >= 4 && !hasNodeMemInfo) {
          memoryTotal = actor.nodeMem[0]; // Total memory
          memoryAvailable = actor.nodeMem[1]; // Available memory
          hasNodeMemInfo = true;
        }
      });

      // If we have node memory info, sum up memory usage from actors in this placement group
      if (hasNodeMemInfo) {
        Object.values(nodeData.actors).forEach((actor: Actor) => {
          if (actor?.placementGroup?.id?.toLowerCase() === pgIdLower) {
            // First check if actor has resource_usage field for Memory
            const resourceUsage = getResourceUsageFromField(
              actor,
              resourceType,
            );
            if (resourceUsage) {
              totalMemoryUsed += resourceUsage.used;
              actorCount++;
            } else if (actor.processStats && actor.processStats.memoryInfo) {
              totalMemoryUsed += actor.processStats.memoryInfo.rss;
              actorCount++;
            }
          }
        });

        // If we found at least one actor with memory usage
        if (actorCount > 0) {
          // Cap usage at 100%
          const usage = Math.min(totalMemoryUsed / memoryTotal, 1);

          return {
            available: Math.max(memoryAvailable, 0),
            total: memoryTotal,
            used: totalMemoryUsed,
            usage: usage,
          };
        }
      }

      // If no node memory info or no actors with memory usage, don't show memory usage
      return null;
    }
    // For custom resources from resource_usage field
    else if (nodeData?.actors) {
      let totalUsed = 0;
      let totalAvailable = 0;
      let actorCount = 0;

      // Sum up resource usage from all actors in this placement group
      Object.values(nodeData.actors).forEach((actor: Actor) => {
        if (actor?.placementGroup?.id?.toLowerCase() === pgIdLower) {
          const resourceUsage = getResourceUsageFromField(actor, resourceType);
          if (resourceUsage) {
            totalUsed += resourceUsage.used;
            totalAvailable += resourceUsage.total;
            actorCount++;
          }
        }
      });

      // If we found any resource usage
      if (actorCount > 0) {
        // Cap usage at 100%
        const usage = Math.min(totalUsed / totalAvailable, 1);

        return {
          available: Math.max(totalAvailable - totalUsed, 0),
          total: totalAvailable,
          used: totalUsed,
          usage: usage,
        };
      }
    }

    // Fallback to original resource calculation if no specific calculation was done
    const resourceValue = resources[matchingKey];

    // Cap usage at 100%
    const used = resourceValue.total - resourceValue.available;
    const usage = Math.min(used / resourceValue.total, 1);

    return {
      available: Math.max(resourceValue.available, 0),
      total: resourceValue.total,
      used: used,
      usage: usage,
    };
  }

  return null;
};

// Get available resources for dropdown
const getAvailableResources = (
  resources: Record<string, ResourceValue>,
  physicalViewData: PhysicalViewData,
) => {
  // Start with default resources
  const resourceTypes = [
    {
      key: "GPU",
      label: "GPU Memory",
    },
    {
      key: "GPUUtil",
      label: "GPU Utilization",
    },
    {
      key: "CPU",
      label: "CPU Usage",
    },
    {
      key: "Memory",
      label: "Memory Usage",
    },
  ];

  // Add custom resources from resource_usage field
  const customResources = new Set<string>();

  if (physicalViewData?.physicalView) {
    Object.values(physicalViewData.physicalView).forEach((nodeData) => {
      if (nodeData?.actors) {
        Object.values(nodeData.actors).forEach((actor: any) => {
          if (actor?.resourceUsage) {
            // Add all resource keys from resource_usage
            Object.keys(actor.resourceUsage).forEach((key) => {
              customResources.add(key);
            });
          }
        });
      }
    });
  }

  // Add custom resources to the list
  customResources.forEach((resource) => {
    // Only add if not already in the list
    if (
      !resourceTypes.some((r) => r.key.toLowerCase() === resource.toLowerCase())
    ) {
      resourceTypes.push({
        key: resource,
        label: `${resource.charAt(0).toUpperCase() + resource.slice(1)} Usage`,
      });
    }
  });

  return resourceTypes;
};

// Utility function to get unique context keys
const getAvailableContextKeys = (physicalViewData: PhysicalViewData) => {
  const contextKeys = new Set<string>();
  contextKeys.add("actor_id"); // Always add actor_id as an option
  contextKeys.add("actor_name"); // Always add actor_name as an option

  if (physicalViewData?.physicalView) {
    Object.values(physicalViewData.physicalView).forEach((nodeData) => {
      if (nodeData?.actors) {
        Object.values(nodeData.actors).forEach((actor: any) => {
          if (actor?.contextInfo) {
            Object.keys(actor.contextInfo).forEach((key) =>
              contextKeys.add(key),
            );
          }
        });
      }
    });
  }

  return Array.from(contextKeys)
    .sort()
    .map((key) => ({
      key,
      label:
        key === "actor_id"
          ? "Actor ID"
          : key === "actor_name"
          ? "Actor Name"
          : key,
    }));
};

// Utility function to get all unique context values for a key
const getUniqueContextValues = (
  physicalViewData: PhysicalViewData,
  contextKey: string,
) => {
  const values = new Set<string>();

  if (physicalViewData?.physicalView) {
    Object.values(physicalViewData.physicalView).forEach((nodeData) => {
      if (nodeData?.actors) {
        Object.values(nodeData.actors).forEach((actor: any) => {
          if (contextKey === "actor_id") {
            if (actor?.actorId) {
              values.add(actor.actorId);
            }
          } else if (contextKey === "actor_name") {
            if (actor?.name) {
              values.add(actor.name);
            } else {
              values.add("Unknown");
            }
          } else if (actor?.contextInfo?.[contextKey] !== undefined) {
            values.add(actor.contextInfo[contextKey].toString());
          }
        });
      }
    });
  }

  return Array.from(values);
};

// Create color scale for context values
const getContextColorScale = (values: string[]) => {
  const colorScale = d3
    .scaleOrdinal<string>()
    .domain(values)
    .range(d3.schemeCategory10);
  return colorScale;
};

// Add Actor type definition
type Actor = {
  actorId: string;
  name: string;
  state: string;
  pid?: number;
  nodeId?: string;
  devices?: string[];
  gpuDevices?: Array<{
    index: number;
    name: string;
    uuid: string;
    memoryUsed: number;
    memoryTotal: number;
    utilization: number;
  }>;
  requiredResources?: Record<string, number>;
  placementGroup?: {
    id: string;
  };
  contextInfo?: Record<string, any>;
  processStats?: {
    cpuPercent: number;
    memoryInfo: {
      rss: number;
      vms?: number;
      shared?: number;
      text?: number;
      lib?: number;
      data?: number;
      dirty?: number;
    };
  };
  nodeCpuPercent?: number;
  nodeMem?: number[];
  mem?: number[];
  resourceUsage?: Record<
    string,
    {
      used: number;
      base: string;
    }
  >;
};

// Add NodeData type definition
type NodeData = {
  resources: Record<string, ResourceValue>;
  actors: Record<string, Actor>;
  gpus?: Array<{
    index: number;
    name: string;
    uuid: string;
    utilizationGpu: number;
    memoryUsed: number;
    memoryTotal: number;
    processesPids: Array<{
      pid: number;
      gpuMemoryUsage: number;
    }>;
  }>;
};

type PhysicalVisualizationProps = {
  physicalViewData: PhysicalViewData;
  onElementClick: (data: any, skip_zoom: boolean) => void;
  selectedElementId: string | null;
  jobId?: string;
  onUpdate?: () => void;
  updating?: boolean;
  searchTerm?: string;
};

// Add helper functions at the top level, before the PhysicalVisualization component
// Helper function to calculate GPU usage for a single actor
const getActorGpuUsage = (actor: Actor) => {
  if (!actor.gpuDevices || actor.gpuDevices.length === 0) {
    return null;
  }

  let totalMemoryUsed = 0;
  let totalMemoryAvailable = 0;

  actor.gpuDevices.forEach((gpu) => {
    totalMemoryUsed += gpu.memoryUsed;
    if (totalMemoryAvailable === 0) {
      totalMemoryAvailable = gpu.memoryTotal;
    }
  });

  if (totalMemoryAvailable === 0) {
    return null;
  }

  // Cap usage at 100%
  const usage = Math.min(totalMemoryUsed / totalMemoryAvailable, 1);

  return {
    available: Math.max(totalMemoryAvailable - totalMemoryUsed, 0),
    total: totalMemoryAvailable,
    used: totalMemoryUsed,
    usage: usage,
  };
};

// Helper function to calculate GPU utilization for a single actor
const getActorGpuUtilization = (actor: Actor) => {
  if (!actor.gpuDevices || actor.gpuDevices.length === 0) {
    return null;
  }

  let totalUtilization = 0;
  let deviceCount = 0;

  actor.gpuDevices.forEach((gpu) => {
    if (gpu.utilization !== undefined) {
      totalUtilization += gpu.utilization;
      deviceCount++;
    }
  });

  if (deviceCount === 0) {
    return null;
  }

  // Calculate average utilization as a percentage
  const avgUtilization = totalUtilization / deviceCount;

  // Cap usage at 100%
  const usage = Math.min(avgUtilization / 100, 1);

  return {
    available: 100 - avgUtilization,
    total: 100,
    used: avgUtilization,
    usage: usage,
  };
};

// Helper function to calculate CPU usage for a single actor
const getActorCpuUsage = (actor: Actor) => {
  // Use nodeCpuPercent if available
  if (actor.nodeCpuPercent !== undefined) {
    const cpuPercent = actor.nodeCpuPercent;

    // Cap usage at 100%
    const cappedPercent = Math.min(cpuPercent, 100);

    return {
      available: 100 - cappedPercent,
      total: 100,
      used: cpuPercent, // Keep original value for display
      usage: cappedPercent / 100,
    };
  }

  if (!actor.processStats || actor.processStats.cpuPercent === undefined) {
    return null;
  }

  const cpuPercent = actor.processStats.cpuPercent;

  // Cap usage at 100%
  const cappedPercent = Math.min(cpuPercent, 100);

  return {
    available: 100 - cappedPercent,
    total: 100,
    used: cpuPercent, // Keep original value for display
    usage: cappedPercent / 100,
  };
};

// Helper function to calculate memory usage for a single actor
const getActorMemoryUsage = (actor: Actor) => {
  if (!actor.processStats || !actor.processStats.memoryInfo) {
    return null;
  }

  // Only use nodeMem if available - no estimation
  if (actor.nodeMem && actor.nodeMem.length >= 4) {
    const memoryUsed = actor.processStats.memoryInfo.rss;
    const memoryTotal = actor.nodeMem[0]; // Total memory
    const memoryAvailable = actor.nodeMem[1]; // Available memory

    // Cap usage at 100%
    const usage = Math.min(memoryUsed / memoryTotal, 1);

    return {
      available: Math.max(memoryAvailable, 0),
      total: memoryTotal,
      used: memoryUsed,
      usage: usage,
    };
  }

  // If we don't have node memory information, return null
  return null;
};

// Helper function to calculate node-level GPU usage
const getNodeGpuUsage = (nodeData: NodeData) => {
  if (!nodeData.gpus || nodeData.gpus.length === 0) {
    return null;
  }

  let totalMemoryUsed = 0;
  let totalMemoryAvailable = 0;

  nodeData.gpus.forEach((gpu) => {
    totalMemoryUsed += gpu.memoryUsed;
    totalMemoryAvailable += gpu.memoryTotal;
  });

  if (totalMemoryAvailable === 0) {
    return null;
  }

  // Cap usage at 100%
  const usage = Math.min(totalMemoryUsed / totalMemoryAvailable, 1);

  return {
    available: Math.max(totalMemoryAvailable - totalMemoryUsed, 0),
    total: totalMemoryAvailable,
    used: totalMemoryUsed,
    usage: usage,
  };
};

// Helper function to calculate node-level CPU usage
const getNodeCpuUsage = (nodeData: NodeData) => {
  if (!nodeData.actors) {
    return null;
  }

  let totalCpuPercent = 0;
  let actorCount = 0;
  let hasNodeCpuInfo = false;

  Object.values(nodeData.actors).forEach((actor) => {
    // If any actor has nodeCpuPercent, use that instead of summing individual CPU usages
    if (actor.nodeCpuPercent !== undefined) {
      totalCpuPercent = actor.nodeCpuPercent;
      hasNodeCpuInfo = true;
      return; // Exit the loop early once we find node CPU info
    }

    if (actor.processStats && actor.processStats.cpuPercent !== undefined) {
      totalCpuPercent += actor.processStats.cpuPercent;
      actorCount++;
    }
  });

  if (!hasNodeCpuInfo && actorCount === 0) {
    return null;
  }

  // Cap at 100% for visualization purposes
  const cappedUsage = Math.min(totalCpuPercent, 100);

  return {
    available: 100 - cappedUsage,
    total: 100,
    used: totalCpuPercent, // Keep original value for display
    usage: cappedUsage / 100,
  };
};

// Helper function to calculate node-level memory usage
const getNodeMemoryUsage = (nodeData: NodeData) => {
  if (!nodeData.actors) {
    return null;
  }

  let hasNodeMemInfo = false;
  let memoryTotal = 0;
  let memoryAvailable = 0;

  // Try to get node memory info from any actor
  Object.values(nodeData.actors).forEach((actor) => {
    if (actor.nodeMem && actor.nodeMem.length >= 4 && !hasNodeMemInfo) {
      memoryTotal = actor.nodeMem[0]; // Total memory
      memoryAvailable = actor.nodeMem[1]; // Available memory
      hasNodeMemInfo = true;
    }
  });

  if (hasNodeMemInfo) {
    const memoryUsed = memoryTotal - memoryAvailable;

    // Cap usage at 100%
    const usage = Math.min(memoryUsed / memoryTotal, 1);

    return {
      available: Math.max(memoryAvailable, 0),
      total: memoryTotal,
      used: memoryUsed,
      usage: usage,
    };
  }

  // If no node memory info, don't show memory usage
  return null;
};

// Helper function to calculate node-level GPU utilization
const getNodeGpuUtilization = (nodeData: NodeData) => {
  if (!nodeData.gpus || nodeData.gpus.length === 0) {
    // Try accessing gpu utilization from actors instead
    if (nodeData.actors) {
      let totalUtilization = 0;
      let deviceCount = 0;

      Object.values(nodeData.actors).forEach((actor) => {
        if (actor.gpuDevices) {
          actor.gpuDevices.forEach((gpu) => {
            if (gpu.utilization !== undefined) {
              totalUtilization += gpu.utilization;
              deviceCount++;
            }
          });
        }
      });

      if (deviceCount > 0) {
        const avgUtilization = totalUtilization / deviceCount;
        const usage = Math.min(avgUtilization / 100, 1);

        return {
          available: 100 - avgUtilization,
          total: 100,
          used: avgUtilization,
          usage: usage,
        };
      }
    }
    return null;
  }

  let totalUtilization = 0;
  let deviceCount = 0;

  nodeData.gpus.forEach((gpu) => {
    if (gpu.utilizationGpu !== undefined) {
      totalUtilization += gpu.utilizationGpu;
      deviceCount++;
    }
  });

  if (deviceCount === 0) {
    return null;
  }

  // Calculate average utilization as a percentage
  const avgUtilization = totalUtilization / deviceCount;

  // Cap usage at 100%
  const usage = Math.min(avgUtilization / 100, 1);

  return {
    available: 100 - avgUtilization,
    total: 100,
    used: avgUtilization,
    usage: usage,
  };
};

// Helper function to check if actor or node has resource info
const hasResourceInfo = (
  data: Actor | NodeData,
  resourceType: string,
): boolean => {
  if (resourceType.toLowerCase() === "gpu") {
    if ((data as Actor).gpuDevices) {
      return ((data as Actor).gpuDevices?.length ?? 0) > 0;
    }
    if ((data as NodeData).gpus) {
      return ((data as NodeData).gpus?.length ?? 0) > 0;
    }
  } else if (resourceType.toLowerCase() === "gpuutil") {
    if ((data as Actor).gpuDevices) {
      return (
        (data as Actor).gpuDevices?.some(
          (gpu) => gpu.utilization !== undefined,
        ) ?? false
      );
    }
    if ((data as NodeData).gpus) {
      return (
        (data as NodeData).gpus?.some(
          (gpu) => gpu.utilizationGpu !== undefined,
        ) ?? false
      );
    }
    if ((data as NodeData).actors) {
      return Object.values((data as NodeData).actors).some(
        (actor) =>
          actor.gpuDevices?.some((gpu) => gpu.utilization !== undefined) ??
          false,
      );
    }
  } else if (resourceType.toLowerCase() === "cpu") {
    if ((data as Actor).processStats) {
      return (data as Actor).processStats?.cpuPercent !== undefined;
    }
    if ((data as NodeData).actors) {
      return Object.values((data as NodeData).actors).some(
        (actor) => actor.processStats?.cpuPercent !== undefined,
      );
    }
  } else if (resourceType.toLowerCase() === "memory") {
    // Update memory check to only require processStats.memoryInfo
    if ((data as Actor).processStats) {
      return (data as Actor).processStats?.memoryInfo?.rss !== undefined;
    }
    if ((data as NodeData).actors) {
      return Object.values((data as NodeData).actors).some(
        (actor) => actor.processStats?.memoryInfo?.rss !== undefined,
      );
    }
  }

  // Check for custom resources in resourceUsage field
  if ((data as Actor).resourceUsage) {
    return (data as Actor).resourceUsage?.[resourceType] !== undefined;
  }

  return false;
};

// Constants for actor dimensions
const ACTOR_HEIGHT = 24; // Fixed height for all actors
const ACTOR_WIDTH = ACTOR_HEIGHT * 6; // Width is 6 times the height

// Extend this to include an exportSvg method
export type PhysicalVisualizationHandle = {
  exportSvg: () => void;
};

const PhysicalVisualization = forwardRef<
  PhysicalVisualizationHandle,
  PhysicalVisualizationProps
>(
  (
    {
      physicalViewData,
      onElementClick,
      selectedElementId,
      jobId,
      onUpdate,
      updating = false,
      searchTerm = "",
    },
    ref,
  ) => {
    const svgRef = useRef<SVGSVGElement | null>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const zoomRef =
      useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
    const graphRef =
      useRef<d3.Selection<SVGGElement, unknown, null, any> | null>(null);
    const [selectedResource, setSelectedResource] = useState<string>("GPU");
    const [selectedContext, setSelectedContext] =
      useState<string>("actor_name");
    const [contextValueFilter, setContextValueFilter] = useState<string>("");
    const legendRef = useRef<SVGGElement | null>(null);

    // Move colors into useMemo
    const colors = useMemo(
      () => ({
        node: "#e8f5e9",
        nodeStroke: "#2e7d32",
        placementGroup: "#bbdefb",
        placementGroupStroke: "#1976d2",
        freeActors: "#ffecb3",
        freeActorsStroke: "#ff8f00",
        actor: "#c5e1a5",
        actorStroke: "#558b2f",
        selectedElement: "#ff5722",
        actorSection: "#ffffff",
        actorSectionStroke: "#90caf9",
      }),
      [],
    );

    const renderLegend = useCallback(
      (contextKey: string) => {
        if (!svgRef.current || !physicalViewData) {
          return;
        }

        // Remove existing legend
        if (legendRef.current) {
          d3.select(legendRef.current).remove();
        }

        const svg = d3.select(svgRef.current);
        const values = getUniqueContextValues(physicalViewData, contextKey);
        const colorScale = getContextColorScale(values);

        // Create a foreign object to hold the HTML-based legend
        const svgHeight = parseInt(svg.style("height"));
        const legendWidth = 200;
        const legendHeight = Math.min(300, svgHeight - 70); // Cap height and allow scrolling
        const legendX = 20;
        const legendY = 50;

        // Add legend container as a foreignObject for HTML content
        const foreignObject = svg
          .append("foreignObject")
          .attr("x", legendX - 10)
          .attr("y", legendY - 25)
          .attr("width", legendWidth + 20)
          .attr("height", legendHeight + 30);

        // Keep a reference to be able to remove it later
        legendRef.current = foreignObject.node() as SVGGElement;

        // Create HTML content for the legend
        const legendDiv = foreignObject
          .append("xhtml:div")
          .style("width", "100%")
          .style("height", "100%")
          .style("background", "white")
          .style("border", "1px solid #ccc")
          .style("border-radius", "5px")
          .style("padding", "10px")
          .style("box-sizing", "border-box");

        // Add legend title
        legendDiv
          .append("xhtml:div")
          .style("font-size", "12px")
          .style("font-weight", "bold")
          .style("margin-bottom", "10px")
          .text(contextKey === "actor_id" ? "Actor ID" : contextKey);

        // Create scrollable container for legend items
        const itemsContainer = legendDiv
          .append("xhtml:div")
          .style("max-height", `${legendHeight - 40}px`) // Reduce max-height to ensure scrolling works
          .style("overflow-y", "auto")
          .style("overflow-x", "hidden")
          .style("padding-right", "5px") // Add padding for scrollbar
          .style("margin-right", "-5px") // Offset padding to maintain alignment
          .on("wheel", (event) => {
            // Prevent scroll events from propagating to the SVG
            event.stopPropagation();
          })
          .on("mousewheel", (event) => {
            // For older browsers
            event.stopPropagation();
          })
          .on("DOMMouseScroll", (event) => {
            // For Firefox
            event.stopPropagation();
          });

        // Add legend items vertically
        values.forEach((value) => {
          const itemDiv = itemsContainer
            .append("xhtml:div")
            .style("display", "flex")
            .style("align-items", "center")
            .style("padding", "4px 0")
            .style("white-space", "nowrap")
            .style("text-overflow", "ellipsis");

          // Color box
          itemDiv
            .append("xhtml:div")
            .style("width", "15px")
            .style("height", "15px")
            .style("background-color", colorScale(value))
            .style("border", "0.5px solid #ccc")
            .style("flex-shrink", "0");

          // Text with tooltip
          itemDiv
            .append("xhtml:div")
            .style("margin-left", "8px")
            .style("overflow", "hidden")
            .style("text-overflow", "ellipsis")
            .style("font-size", "12px")
            .style("width", "calc(100% - 23px)") // Fixed width to ensure overflow works
            .attr("title", value) // Add tooltip
            .text(value);
        });

        // Add search match legend item if there's a search term
        if (searchTerm && searchTerm.trim() !== "") {
          const searchItemDiv = itemsContainer
            .append("xhtml:div")
            .style("display", "flex")
            .style("align-items", "center")
            .style("padding", "4px 0")
            .style("margin-top", "5px");

          // Color box for search matches
          searchItemDiv
            .append("xhtml:div")
            .style("width", "15px")
            .style("height", "15px")
            .style("background-color", "white")
            .style("border", "2px solid #4caf50")
            .style("border-style", "dashed")
            .style("flex-shrink", "0");

          // Text for search matches
          searchItemDiv
            .append("xhtml:div")
            .style("margin-left", "8px")
            .style("font-size", "12px")
            .text("Search Match");
        }
      },
      [physicalViewData, searchTerm],
    );

    // Function to check if an actor matches the search term
    const actorMatchesSearch = useCallback(
      (actor: Actor): boolean => {
        if (!searchTerm || searchTerm.trim() === "") {
          return false;
        }

        const searchTermLower = searchTerm.toLowerCase();

        // Check actor ID
        if (
          actor.actorId &&
          actor.actorId.toLowerCase().includes(searchTermLower)
        ) {
          return true;
        }

        // Check actor name
        if (actor.name && actor.name.toLowerCase().includes(searchTermLower)) {
          return true;
        }

        // Check context info
        if (actor.contextInfo) {
          for (const key in actor.contextInfo) {
            const value = actor.contextInfo[key];
            if (
              value &&
              value.toString().toLowerCase().includes(searchTermLower)
            ) {
              return true;
            }
          }
        }

        return false;
      },
      [searchTerm],
    );

    // Function to check if an actor matches the context value filter
    const actorMatchesContextFilter = useCallback(
      (actor: Actor): boolean => {
        if (!contextValueFilter || contextValueFilter.trim() === "") {
          return true; // No filter applied, all actors match
        }

        const filterLower = contextValueFilter.toLowerCase();

        let contextValue: string | undefined;
        if (selectedContext === "actor_id") {
          contextValue = actor.actorId;
        } else if (selectedContext === "actor_name") {
          contextValue = actor.name || "Unknown";
        } else {
          contextValue = actor.contextInfo?.[selectedContext]?.toString();
        }

        // If context value is undefined or doesn't match the filter, return false
        return (
          contextValue !== undefined &&
          contextValue.toLowerCase().includes(filterLower)
        );
      },
      [contextValueFilter, selectedContext],
    );

    // Add a filter definition for the glow effect
    const addGlowFilter = useCallback(
      (svg: d3.Selection<SVGSVGElement, unknown, null, undefined>) => {
        // Remove any existing filter
        svg.select("defs").remove();

        // Create defs element for filters
        const defs = svg.append("defs");

        // Create filter for glow effect
        const filter = defs
          .append("filter")
          .attr("id", "glow-effect")
          .attr("x", "-50%")
          .attr("y", "-50%")
          .attr("width", "200%")
          .attr("height", "200%");

        // Add blur effect
        filter
          .append("feGaussianBlur")
          .attr("stdDeviation", "3")
          .attr("result", "blur");

        // Add color matrix to make the glow green
        filter
          .append("feColorMatrix")
          .attr("in", "blur")
          .attr("type", "matrix")
          .attr(
            "values",
            "0 0 0 0 0.298 0 0 0 0 0.686 0 0 0 0 0.314 0 0 0 1 0",
          );

        // Merge the original with the glow
        const feMerge = filter.append("feMerge");
        feMerge.append("feMergeNode").attr("in", "colorMatrix");
        feMerge.append("feMergeNode").attr("in", "SourceGraphic");
      },
      [],
    );

    const renderPhysicalView = useCallback(() => {
      if (!svgRef.current || !physicalViewData) {
        return;
      }

      const svg = d3.select(svgRef.current);
      svg.selectAll("*").remove();

      // Add glow filter
      addGlowFilter(svg);

      // Set up zoom behavior with better constraints
      const zoom = d3
        .zoom<SVGSVGElement, unknown>()
        .scaleExtent([0.1, 2])
        .on("zoom", (event) => {
          inner.attr("transform", event.transform);
        });

      zoomRef.current = zoom;
      svg.call(zoom);

      // Create the main group element
      const inner = svg.append("g");
      graphRef.current = inner;

      // Get nodes from physical view data
      const nodes = Object.entries(physicalViewData.physicalView || {});
      if (nodes.length === 0) {
        return;
      }

      // Calculate layout dimensions
      const svgWidth = parseInt(svg.style("width"));
      const svgHeight = parseInt(svg.style("height"));
      const nodeMargin = 50;

      // Calculate vertical layout (single column)
      const rows = nodes.length;

      // Get color scale for current context
      const contextValues = getUniqueContextValues(
        physicalViewData,
        selectedContext,
      );
      const contextColorScale = getContextColorScale(contextValues);

      // Update the actor coloring
      const getActorColor = (actor: Actor): string => {
        if (!actor) {
          return "#cccccc";
        }

        let contextValue;
        if (selectedContext === "actor_id") {
          contextValue = actor.actorId;
        } else if (selectedContext === "actor_name") {
          contextValue = actor.name || "Unknown";
        } else {
          contextValue = actor.contextInfo?.[selectedContext]?.toString();
        }

        return contextValue ? contextColorScale(contextValue) : "#cccccc";
      };

      // Calculate maximum node width based on actors
      let maxNodeWidth = 0;
      nodes.forEach(([_, nodeData]) => {
        let nodeWidth = 0;

        // Group all placement groups by ID
        const placementGroups: { [pgId: string]: Actor[] } = {};
        const freeActors: Actor[] = [];

        // Categorize actors
        if (nodeData?.actors) {
          Object.values(nodeData.actors).forEach((actor) => {
            // Skip actors that don't match the context value filter
            if (!actorMatchesContextFilter(actor)) {
              return;
            }

            if (actor?.placementGroup?.id) {
              const pgId = actor.placementGroup.id;
              if (!placementGroups[pgId]) {
                placementGroups[pgId] = [];
              }
              placementGroups[pgId].push(actor);
            } else {
              freeActors.push(actor);
            }
          });
        }

        // Calculate width for each placement group
        Object.values(placementGroups).forEach((actors) => {
          // Calculate width based on fixed actor dimensions and margins
          const pgWidth =
            actors.length * ACTOR_WIDTH + (actors.length - 1) * 8 + 40; // Add margins and padding
          nodeWidth = Math.max(nodeWidth, pgWidth);
        });

        // Calculate width for free actors
        if (freeActors.length > 0) {
          // Calculate width based on fixed actor dimensions and margins
          const freeActorsWidth =
            freeActors.length * ACTOR_WIDTH + (freeActors.length - 1) * 8 + 40; // Add margins and padding
          nodeWidth = Math.max(nodeWidth, freeActorsWidth);
        }

        // Ensure minimum node width
        nodeWidth = Math.max(nodeWidth, 200);
        maxNodeWidth = Math.max(maxNodeWidth, nodeWidth);
      });

      // Use maxNodeWidth for layout calculations
      const contentWidth = maxNodeWidth + 2 * nodeMargin;

      // Calculate node heights dynamically based on content
      const nodeHeights: number[] = [];
      nodes.forEach(([_, nodeData]) => {
        // Group all placement groups by ID
        const placementGroups: { [pgId: string]: Actor[] } = {};
        const freeActors: Actor[] = [];

        // Categorize actors
        if (nodeData?.actors) {
          Object.values(nodeData.actors).forEach((actor) => {
            // Skip actors that don't match the context value filter
            if (!actorMatchesContextFilter(actor)) {
              return;
            }

            if (actor?.placementGroup?.id) {
              const pgId = actor.placementGroup.id;
              if (!placementGroups[pgId]) {
                placementGroups[pgId] = [];
              }
              placementGroups[pgId].push(actor);
            } else {
              freeActors.push(actor);
            }
          });
        }

        const pgKeys = Object.keys(placementGroups);
        const hasFreeActors = freeActors.length > 0;
        const totalGroups = pgKeys.length + (hasFreeActors ? 1 : 0);

        if (totalGroups === 0) {
          nodeHeights.push(200); // Default height for empty nodes
          return;
        }

        // Constants for height calculation
        const headerHeight = 40; // Space for node header
        const actorBoxHeight = ACTOR_HEIGHT + 16; // Fixed height plus padding
        const actorResourceBarMargin = 4;
        const groupSpacing = 14; // Spacing between groups
        const topMargin = 10; // Top margin inside node

        // Calculate height for placement groups
        const pgTotalHeight =
          pgKeys.length *
          (actorBoxHeight + actorResourceBarMargin + groupSpacing);

        // Calculate height for free actors if present
        const freeActorsHeight = hasFreeActors
          ? actorBoxHeight + actorResourceBarMargin
          : 0;

        // Calculate total node height
        const totalNodeHeight =
          headerHeight + pgTotalHeight + freeActorsHeight + topMargin;

        // Ensure minimum node height
        nodeHeights.push(Math.max(totalNodeHeight, 200));
      });

      // Use the maximum node height for layout calculations
      const maxNodeHeight = Math.max(...nodeHeights);
      const contentHeight = rows * (maxNodeHeight + nodeMargin) + nodeMargin;

      // Calculate required scale to fit everything
      const scaleX = svgWidth / contentWidth;
      const scaleY = svgHeight / contentHeight;
      const finalScale = Math.min(scaleX, scaleY, 1) * 0.9;

      // Center coordinates
      const centerX = svgWidth / 2;
      const centerY = svgHeight / 2;

      // Draw nodes with vertical positioning
      nodes.forEach(([nodeId, nodeData], index) => {
        // Skip nodes that don't have any actors matching the context value filter
        if (contextValueFilter && contextValueFilter.trim() !== "") {
          const hasMatchingActors = Object.values(nodeData?.actors || {}).some(
            (actor) => actorMatchesContextFilter(actor),
          );
          if (!hasMatchingActors) {
            return;
          }
        }

        const x = nodeMargin; // Fixed x position for vertical layout
        const y = index * (maxNodeHeight + nodeMargin) + nodeMargin;

        // Group all placement groups by ID
        const placementGroups: { [pgId: string]: Actor[] } = {};
        const freeActors: Actor[] = [];

        // Categorize actors
        if (nodeData?.actors) {
          Object.values(nodeData.actors).forEach((actor) => {
            // Skip actors that don't match the context value filter
            if (!actorMatchesContextFilter(actor)) {
              return;
            }

            if (actor?.placementGroup?.id) {
              const pgId = actor.placementGroup.id;
              if (!placementGroups[pgId]) {
                placementGroups[pgId] = [];
              }
              placementGroups[pgId].push(actor);
            } else {
              freeActors.push(actor);
            }
          });
        }

        const pgKeys = Object.keys(placementGroups);
        const hasFreeActors = freeActors.length > 0;
        const totalGroups = pgKeys.length + (hasFreeActors ? 1 : 0);

        // Use the calculated height for this node
        const nodeHeight = nodeHeights[index];

        // Draw node rectangle
        const nodeGroup = inner
          .append("g")
          .attr("transform", `translate(${x}, ${y})`)
          .attr("class", "node")
          .attr("data-id", nodeId)
          .on("click", (event) => {
            event.stopPropagation();
            onElementClick({ id: nodeId, type: "node", data: nodeData }, true);
          });

        // Get node-level GPU usage if resource is selected
        let nodeGpuInfo = null;
        if (selectedResource && selectedResource.toLowerCase() === "gpu") {
          nodeGpuInfo = getNodeGpuUsage(nodeData);
        }

        // Get node-level CPU usage if resource is selected
        let nodeCpuInfo = null;
        if (selectedResource && selectedResource.toLowerCase() === "cpu") {
          nodeCpuInfo = getNodeCpuUsage(nodeData);
        }

        // Get node-level memory usage if resource is selected
        let nodeMemoryInfo = null;
        if (selectedResource && selectedResource.toLowerCase() === "memory") {
          nodeMemoryInfo = getNodeMemoryUsage(nodeData);
        }

        // Draw the background rectangle (empty part)
        nodeGroup
          .append("rect")
          .attr("width", maxNodeWidth)
          .attr("height", nodeHeight)
          .attr("rx", 5)
          .attr("ry", 5)
          .attr("fill", "#f5f5f5") // Light background for empty part
          .attr(
            "stroke",
            selectedElementId === nodeId
              ? colors.selectedElement
              : colors.nodeStroke,
          )
          .attr("stroke-width", selectedElementId === nodeId ? 3 : 1);

        // Draw the filled part based on resource usage
        if (nodeGpuInfo && nodeGpuInfo.usage > 0) {
          nodeGroup
            .append("rect")
            .attr("width", maxNodeWidth * nodeGpuInfo.usage)
            .attr("height", nodeHeight)
            .attr("rx", 5)
            .attr("ry", 5)
            .attr("fill", colors.node)
            .attr("stroke", "none");

          // Add dashed line divider at the boundary
          nodeGroup
            .append("line")
            .attr("x1", maxNodeWidth * nodeGpuInfo.usage)
            .attr("y1", 0)
            .attr("x2", maxNodeWidth * nodeGpuInfo.usage)
            .attr("y2", nodeHeight)
            .attr("stroke", "#666")
            .attr("stroke-width", 1)
            .attr("stroke-dasharray", "4,2");
        } else if (
          selectedResource &&
          selectedResource.toLowerCase() === "gpuutil"
        ) {
          // Get node-level GPU utilization if resource is selected
          const nodeGpuUtilInfo = getNodeGpuUtilization(nodeData);
          if (nodeGpuUtilInfo && nodeGpuUtilInfo.usage > 0) {
            nodeGroup
              .append("rect")
              .attr("width", maxNodeWidth * nodeGpuUtilInfo.usage)
              .attr("height", nodeHeight)
              .attr("rx", 5)
              .attr("ry", 5)
              .attr("fill", colors.node)
              .attr("stroke", "none");

            // Add dashed line divider at the boundary
            nodeGroup
              .append("line")
              .attr("x1", maxNodeWidth * nodeGpuUtilInfo.usage)
              .attr("y1", 0)
              .attr("x2", maxNodeWidth * nodeGpuUtilInfo.usage)
              .attr("y2", nodeHeight)
              .attr("stroke", "#666")
              .attr("stroke-width", 1)
              .attr("stroke-dasharray", "4,2");
          }
        } else if (nodeCpuInfo && nodeCpuInfo.usage > 0) {
          nodeGroup
            .append("rect")
            .attr("width", maxNodeWidth * nodeCpuInfo.usage)
            .attr("height", nodeHeight)
            .attr("rx", 5)
            .attr("ry", 5)
            .attr("fill", colors.node)
            .attr("stroke", "none");

          // Add dashed line divider at the boundary
          nodeGroup
            .append("line")
            .attr("x1", maxNodeWidth * nodeCpuInfo.usage)
            .attr("y1", 0)
            .attr("x2", maxNodeWidth * nodeCpuInfo.usage)
            .attr("y2", nodeHeight)
            .attr("stroke", "#666")
            .attr("stroke-width", 1)
            .attr("stroke-dasharray", "4,2");
        } else if (nodeMemoryInfo && nodeMemoryInfo.usage > 0) {
          nodeGroup
            .append("rect")
            .attr("width", maxNodeWidth * nodeMemoryInfo.usage)
            .attr("height", nodeHeight)
            .attr("rx", 5)
            .attr("ry", 5)
            .attr("fill", colors.node)
            .attr("stroke", "none");

          // Add dashed line divider at the boundary
          nodeGroup
            .append("line")
            .attr("x1", maxNodeWidth * nodeMemoryInfo.usage)
            .attr("y1", 0)
            .attr("x2", maxNodeWidth * nodeMemoryInfo.usage)
            .attr("y2", nodeHeight)
            .attr("stroke", "#666")
            .attr("stroke-width", 1)
            .attr("stroke-dasharray", "4,2");
        }

        // Node label
        nodeGroup
          .append("text")
          .attr("x", 10)
          .attr("y", 20)
          .attr("font-weight", "bold")
          .text(`Node: ${nodeId.substring(0, 8)}...`);

        if (totalGroups === 0) {
          return;
        }

        const resourceBarMargin = 12; // Reduced margin
        const pgWidth = maxNodeWidth - 20 - resourceBarMargin;

        // Constants for layout
        const headerHeight = 40; // Space for node header
        const actorBoxHeight = ACTOR_HEIGHT + 16; // Fixed height plus padding
        const actorResourceBarMargin = 4;
        const groupSpacing = 14; // Spacing between groups

        // Calculate total height for each placement group
        const pgTotalHeight = actorBoxHeight + actorResourceBarMargin;

        // Use the new rendering functions with all required parameters
        renderPlacementGroups(
          nodeGroup,
          placementGroups,
          pgKeys,
          nodeData,
          headerHeight, // Start after header
          pgWidth,
          pgTotalHeight,
          colors,
          selectedResource,
          selectedElementId,
          onElementClick,
          getActorColor,
          actorMatchesSearch,
          searchTerm,
          groupSpacing, // Pass group spacing as parameter
          selectedContext,
        );

        // Calculate Y position for free actors
        const freeActorsY =
          headerHeight +
          (pgKeys.length > 0
            ? pgKeys.length * (pgTotalHeight + groupSpacing)
            : 0);

        renderFreeActors(
          nodeGroup,
          freeActors,
          freeActorsY,
          pgWidth,
          pgTotalHeight,
          colors,
          selectedElementId,
          onElementClick,
          getActorColor,
          actorMatchesSearch,
          selectedResource,
          searchTerm,
          selectedContext,
        );
      });

      // After all nodes are drawn, center the visualization
      svg.call(
        zoom.transform,
        d3.zoomIdentity
          .translate(centerX, centerY)
          .scale(finalScale)
          .translate(-contentWidth / 2, -contentHeight / 2),
      );

      // Set viewBox to contain the graph
      svg.attr("viewBox", `0 0 ${svgWidth} ${svgHeight}`);

      // Render legend after graph
      renderLegend(selectedContext);

      // Ensure search highlights are visible by raising them to the top
      if (searchTerm && searchTerm.trim() !== "") {
        svg.selectAll("rect[stroke='#4caf50']").raise();
      }
    }, [
      physicalViewData,
      selectedElementId,
      onElementClick,
      selectedResource,
      selectedContext,
      renderLegend,
      colors,
      actorMatchesSearch,
      searchTerm,
      addGlowFilter,
      actorMatchesContextFilter,
      contextValueFilter, // Add contextValueFilter to dependency array
    ]);

    // Initial render and on data change
    useEffect(() => {
      renderPhysicalView();
    }, [renderPhysicalView, physicalViewData, contextValueFilter]);

    // Function to export the SVG visualization
    const exportSvg = () => {
      if (!svgRef.current) {
        return;
      }

      // Get the SVG element
      const svgElement = svgRef.current;

      // Create a copy of the SVG to avoid modifying the original
      const svgCopy = svgElement.cloneNode(true) as SVGSVGElement;

      // Set the proper dimensions and styling
      svgCopy.setAttribute("xmlns", "http://www.w3.org/2000/svg");
      svgCopy.setAttribute("width", svgElement.clientWidth.toString());
      svgCopy.setAttribute("height", svgElement.clientHeight.toString());

      // Convert to a string
      const serializer = new XMLSerializer();
      const svgString = serializer.serializeToString(svgCopy);

      // Create a blob from the SVG string
      const blob = new Blob([svgString], { type: "image/svg+xml" });
      const url = URL.createObjectURL(blob);

      // Create a download link and trigger the download
      const a = document.createElement("a");
      a.href = url;
      a.download = `ray-physical-visualization-${new Date()
        .toISOString()
        .slice(0, 10)}.svg`;
      document.body.appendChild(a);
      a.click();

      // Clean up
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    };

    // Expose exportSvg method
    useImperativeHandle(ref, () => ({
      exportSvg,
    }));

    return (
      <div
        ref={containerRef}
        className="ray-visualization-container"
        style={{ display: "flex", flexDirection: "column" }}
      >
        <div
          className="graph-container"
          style={{ flex: "1 1 auto", position: "relative" }}
        >
          <svg ref={svgRef} width="100%" height="600"></svg>
          <div
            className="resource-selector"
            style={{
              position: "absolute",
              top: "10px",
              right: "100px",
              padding: "10px",
              display: "flex",
              flexDirection: "row",
              alignItems: "center",
              gap: "10px",
              backgroundColor: "rgba(245, 245, 245, 0.9)",
              borderRadius: "4px",
              boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
              zIndex: 10,
            }}
          >
            <FormControl size="small" style={{ width: "180px" }}>
              <InputLabel>Resource Type</InputLabel>
              <Select
                value={selectedResource}
                onChange={(e) => setSelectedResource(e.target.value)}
                label="Resource Type"
                sx={{
                  "& .MuiSelect-select": {
                    paddingRight: "32px !important",
                  },
                }}
                IconComponent={() => (
                  <div
                    style={{
                      position: "absolute",
                      right: "7px",
                      top: "50%",
                      transform: "translateY(-50%)",
                      pointerEvents: "none",
                    }}
                  >
                    <svg
                      width="12"
                      height="12"
                      viewBox="0 0 24 24"
                      fill="currentColor"
                    >
                      <path d="M7 10l5 5 5-5z" />
                    </svg>
                  </div>
                )}
              >
                {getAvailableResources(
                  physicalViewData.physicalView?.[
                    Object.keys(physicalViewData.physicalView)[0]
                  ]?.resources || {},
                  physicalViewData,
                ).map(({ key, label }) => (
                  <MenuItem key={key} value={key}>
                    {label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <FormControl size="small" style={{ width: "180px" }}>
              <InputLabel>Context</InputLabel>
              <Select
                value={selectedContext}
                onChange={(e) => {
                  setSelectedContext(e.target.value);
                  setContextValueFilter(""); // Reset filter when context changes
                }}
                label="Context"
                sx={{
                  "& .MuiSelect-select": {
                    paddingRight: "32px !important",
                  },
                }}
                IconComponent={() => (
                  <div
                    style={{
                      position: "absolute",
                      right: "7px",
                      top: "50%",
                      transform: "translateY(-50%)",
                      pointerEvents: "none",
                    }}
                  >
                    <svg
                      width="12"
                      height="12"
                      viewBox="0 0 24 24"
                      fill="currentColor"
                    >
                      <path d="M7 10l5 5 5-5z" />
                    </svg>
                  </div>
                )}
              >
                {getAvailableContextKeys(physicalViewData).map(
                  ({ key, label }) => (
                    <MenuItem key={key} value={key}>
                      {label}
                    </MenuItem>
                  ),
                )}
              </Select>
            </FormControl>
            <FormControl size="small" style={{ width: "180px" }}>
              <TextField
                label="Filter Context Value"
                value={contextValueFilter}
                onChange={(e) => setContextValueFilter(e.target.value)}
                placeholder={`Filter by ${selectedContext}`}
                variant="outlined"
                size="small"
              />
            </FormControl>
          </div>
        </div>
      </div>
    );
  },
);

// Helper function to get context value for an actor (move before renderPlacementGroups)
const getActorContextValue = (actor: Actor, contextKey: string): string => {
  if (contextKey === "actor_id") {
    return actor.actorId || "unknown";
  } else if (contextKey === "actor_name") {
    return actor.name || "Unknown";
  } else if (actor.contextInfo && actor.contextInfo[contextKey] !== undefined) {
    return actor.contextInfo[contextKey].toString();
  }
  return "Unknown";
};

// Update renderPlacementGroups signature to include selectedContext
const renderPlacementGroups = (
  nodeGroup: any,
  placementGroups: Record<string, Actor[]>,
  pgKeys: string[],
  nodeData: NodeData,
  pgY: number,
  pgWidth: number,
  pgHeight: number,
  colors: any,
  selectedResource: string,
  selectedElementId: string | null,
  onElementClick: (data: any, skip_zoom: boolean) => void,
  getActorColor: (actor: Actor) => string,
  actorMatchesSearchFn: (actor: Actor) => boolean,
  searchTerm: string | undefined,
  groupSpacing: number,
  selectedContext: string,
) => {
  const actorMargin = 8; // Add margin between actors
  const topMargin = 4; // Add top margin
  const actorResourceBarMargin = 4; // Margin between actor and its resource bar
  const pgPadding = 20; // Padding inside placement group (10px on each side)

  // Available width for placement groups (accounting for node padding)
  const availableWidth = pgWidth - pgPadding;

  pgKeys.forEach((pgId, pgIndex) => {
    const actors = placementGroups[pgId];
    const currentY = pgY + pgIndex * (pgHeight + groupSpacing); // Increased spacing

    // Calculate total width needed for all actors including margins
    const totalActorsWidth =
      actors.length * ACTOR_WIDTH + (actors.length - 1) * actorMargin;

    // Calculate scale factor if total width exceeds available width
    let containerWidth = totalActorsWidth + 20; // Add some padding
    let scaleFactor = 1;

    if (containerWidth > availableWidth) {
      scaleFactor = availableWidth / containerWidth;
      containerWidth = availableWidth;
    }

    // Calculate the total height needed for the placement group
    const actorBoxHeight = ACTOR_HEIGHT + 16; // Fixed height plus padding
    const totalPGHeight = actorBoxHeight + actorResourceBarMargin;

    // Placement group rectangle
    const pgGroup = nodeGroup
      .append("g")
      .attr("transform", `translate(10, ${currentY + topMargin})`)
      .attr("class", "placement-group")
      .attr("data-id", pgId);

    // Get resource usage information for this placement group
    let resourceInfo = null;
    if (selectedResource) {
      resourceInfo = extractResourceUsage(
        nodeData.resources,
        pgId,
        selectedResource,
        nodeData,
      );
    }

    // Draw the background rectangle (empty part)
    pgGroup
      .append("rect")
      .attr("width", containerWidth)
      .attr("height", totalPGHeight)
      .attr("rx", 3)
      .attr("ry", 3)
      .attr("fill", "#f5f5f5") // Light background for empty part
      .attr("stroke", colors.placementGroupStroke)
      .attr("stroke-width", 1);

    // Draw the filled part based on resource usage
    if (resourceInfo && resourceInfo.usage > 0) {
      pgGroup
        .append("rect")
        .attr("width", containerWidth * resourceInfo.usage)
        .attr("height", totalPGHeight)
        .attr("rx", 3)
        .attr("ry", 3)
        .attr("fill", colors.placementGroup)
        .attr("stroke", "none");

      // Add dashed line divider at the boundary
      pgGroup
        .append("line")
        .attr("x1", containerWidth * resourceInfo.usage)
        .attr("y1", 0)
        .attr("x2", containerWidth * resourceInfo.usage)
        .attr("y2", totalPGHeight)
        .attr("stroke", "#666")
        .attr("stroke-width", 1)
        .attr("stroke-dasharray", "4,2");
    }

    // Calculate starting X position to center actors
    const startX = (containerWidth - totalActorsWidth * scaleFactor) / 2;

    // Draw actor sections
    let currentX = startX;
    actors.forEach((actor: Actor, actorIndex: number) => {
      if (!actor) {
        return;
      }

      const actorWidth = ACTOR_WIDTH * scaleFactor;

      // Create clickable section group
      const sectionGroup = pgGroup
        .append("g")
        .attr("transform", `translate(${currentX}, 0)`)
        .attr("class", "actor-section")
        .attr("data-id", actor.actorId || "unknown")
        .on("click", (event: any) => {
          event.stopPropagation();
          onElementClick(
            {
              id: actor.actorId || "unknown",
              type: "actor",
              name: actor.name || `Actor${actorIndex + 1}`,
              language: "python",
              devices: actor.devices || [],
              gpuDevices: actor.gpuDevices || [],
              state: actor.state,
              pid: actor.pid,
              nodeId: actor.nodeId,
              requiredResources: actor.requiredResources,
              data: actor,
            },
            true,
          );
        });

      // Calculate actor dimensions with padding
      const padding = 8;
      const actorHeight = ACTOR_HEIGHT;

      // Get actor-level resource usage based on selected resource type
      let actorResourceInfo = null;
      if (selectedResource) {
        if (
          selectedResource.toLowerCase() === "gpu" &&
          hasResourceInfo(actor, "gpu")
        ) {
          actorResourceInfo = getActorGpuUsage(actor);
        } else if (
          selectedResource.toLowerCase() === "gpuutil" &&
          hasResourceInfo(actor, "gpuutil")
        ) {
          actorResourceInfo = getActorGpuUtilization(actor);
        } else if (
          selectedResource.toLowerCase() === "cpu" &&
          hasResourceInfo(actor, "cpu")
        ) {
          actorResourceInfo = getActorCpuUsage(actor);
        } else if (
          selectedResource.toLowerCase() === "memory" &&
          hasResourceInfo(actor, "memory")
        ) {
          actorResourceInfo = getActorMemoryUsage(actor);
        } else if (hasResourceInfo(actor, selectedResource)) {
          // Use getResourceUsageFromField for custom resources
          actorResourceInfo = getResourceUsageFromField(
            actor,
            selectedResource,
          );
        }
      }

      // Draw actor background (empty part)
      sectionGroup
        .append("rect")
        .attr("x", 0)
        .attr("y", padding)
        .attr("width", actorWidth)
        .attr("height", actorHeight)
        .attr("fill", "#f5f5f5") // Light background for empty part
        .attr("opacity", searchTerm && !actorMatchesSearchFn(actor) ? 0.3 : 1)
        .attr("stroke", "none")
        .attr("rx", 2)
        .attr("ry", 2);

      // Draw filled part based on resource usage
      if (actorResourceInfo && actorResourceInfo.usage > 0) {
        sectionGroup
          .append("rect")
          .attr("x", 0)
          .attr("y", padding)
          .attr("width", actorWidth * actorResourceInfo.usage)
          .attr("height", actorHeight)
          .attr("fill", getActorColor(actor))
          .attr(
            "opacity",
            searchTerm && !actorMatchesSearchFn(actor) ? 0.3 : 0.7,
          )
          .attr("stroke", "none")
          .attr("rx", 2)
          .attr("ry", 2);

        // Add dashed line divider at the boundary
        sectionGroup
          .append("line")
          .attr("x1", actorWidth * actorResourceInfo.usage)
          .attr("y1", padding)
          .attr("x2", actorWidth * actorResourceInfo.usage)
          .attr("y2", padding + actorHeight)
          .attr("stroke", "#666")
          .attr("stroke-width", 1)
          .attr("stroke-dasharray", "3,2");
      }

      // Create a clipping path for text
      const clipId = `actor-text-clip-${pgId}-${actorIndex}`;
      sectionGroup
        .append("clipPath")
        .attr("id", clipId)
        .append("rect")
        .attr("width", actorWidth - 4) // Slight padding
        .attr("height", actorHeight);

      // Add actor label with clipping
      sectionGroup
        .append("text")
        .attr("x", actorWidth / 2)
        .attr("y", actorHeight / 2 + padding)
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .attr("font-size", "10px")
        .attr("fill", "#000000")
        .attr("clip-path", `url(#${clipId})`)
        .text(getActorContextValue(actor, selectedContext))
        .append("title") // Add tooltip for full context value
        .text(getActorContextValue(actor, selectedContext));

      // Add search highlight border if actor matches search
      if (actorMatchesSearchFn(actor)) {
        sectionGroup
          .append("rect")
          .attr("x", 0)
          .attr("y", padding)
          .attr("width", actorWidth)
          .attr("height", actorHeight)
          .attr("fill", "none")
          .attr("stroke", "#4caf50")
          .attr("stroke-width", 2)
          .attr("stroke-dasharray", "4,2")
          .attr("class", "search-highlight")
          .style("pointer-events", "none")
          .attr("rx", 2)
          .attr("ry", 2);
      }

      // Update currentX to include margin for next actor
      currentX += actorWidth + actorMargin;
    });
  });
};

// Update renderFreeActors signature to include selectedContext
const renderFreeActors = (
  nodeGroup: any,
  freeActors: Actor[],
  freeActorsY: number,
  pgWidth: number,
  pgHeight: number,
  colors: any,
  selectedElementId: string | null,
  onElementClick: (data: any, skip_zoom: boolean) => void,
  getActorColor: (actor: Actor) => string,
  actorMatchesSearchFn: (actor: Actor) => boolean,
  selectedResource: string,
  searchTerm: string | undefined,
  selectedContext: string,
) => {
  if (freeActors.length === 0) {
    return;
  }

  const actorMargin = 8;
  const topMargin = 4;
  const groupSpacing = 0; // Add consistent spacing
  const pgPadding = 20; // Padding inside free actors group (10px on each side)

  // Available width for free actors (accounting for node padding)
  const availableWidth = pgWidth - pgPadding;

  // Adjust the starting Y position to account for the increased spacing
  const adjustedFreeActorsY = freeActorsY + groupSpacing;

  const freeActorsGroup = nodeGroup
    .append("g")
    .attr("transform", `translate(10, ${adjustedFreeActorsY + topMargin})`)
    .attr("class", "free-actors");

  // Calculate total width needed for all free actors including margins
  const totalActorsWidth =
    freeActors.length * ACTOR_WIDTH + (freeActors.length - 1) * actorMargin;

  // Calculate scale factor if total width exceeds available width
  let containerWidth = totalActorsWidth + 20; // Add some padding
  let scaleFactor = 1;

  if (containerWidth > availableWidth) {
    scaleFactor = availableWidth / containerWidth;
    containerWidth = availableWidth;
  }

  // Calculate starting X position to center actors
  const startX = (containerWidth - totalActorsWidth * scaleFactor) / 2;

  // Draw free actor sections
  let currentX = startX;
  freeActors.forEach((actor, actorIndex) => {
    if (!actor) {
      return;
    }

    const actorWidth = ACTOR_WIDTH * scaleFactor;

    // Create clickable section group
    const sectionGroup = freeActorsGroup
      .append("g")
      .attr("transform", `translate(${currentX}, 0)`)
      .attr("class", "actor-section")
      .attr("data-id", actor.actorId || "unknown")
      .on("click", (event: any) => {
        event.stopPropagation();
        onElementClick(
          {
            id: actor.actorId || "unknown",
            type: "actor",
            name: actor.name || `Actor${actorIndex + 1}`,
            language: "python",
            devices: actor.devices || [],
            gpuDevices: actor.gpuDevices || [],
            state: actor.state,
            pid: actor.pid,
            nodeId: actor.nodeId,
            requiredResources: actor.requiredResources,
            data: actor,
          },
          true,
        );
      });

    // Calculate actor dimensions with padding
    const padding = 0;
    const actorHeight = ACTOR_HEIGHT;

    // Get actor-level resource usage based on selected resource type
    let actorResourceInfo = null;
    if (selectedResource) {
      if (
        selectedResource.toLowerCase() === "gpu" &&
        hasResourceInfo(actor, "gpu")
      ) {
        actorResourceInfo = getActorGpuUsage(actor);
      } else if (
        selectedResource.toLowerCase() === "gpuutil" &&
        hasResourceInfo(actor, "gpuutil")
      ) {
        actorResourceInfo = getActorGpuUtilization(actor);
      } else if (
        selectedResource.toLowerCase() === "cpu" &&
        hasResourceInfo(actor, "cpu")
      ) {
        actorResourceInfo = getActorCpuUsage(actor);
      } else if (
        selectedResource.toLowerCase() === "memory" &&
        hasResourceInfo(actor, "memory")
      ) {
        actorResourceInfo = getActorMemoryUsage(actor);
      } else if (hasResourceInfo(actor, selectedResource)) {
        // Use getResourceUsageFromField for custom resources
        actorResourceInfo = getResourceUsageFromField(actor, selectedResource);
      }
    }

    // Draw actor background (empty part)
    sectionGroup
      .append("rect")
      .attr("x", 0)
      .attr("y", padding)
      .attr("width", actorWidth)
      .attr("height", actorHeight)
      .attr("fill", "#f5f5f5") // Light background for empty part
      .attr("opacity", searchTerm && !actorMatchesSearchFn(actor) ? 0.3 : 1)
      .attr("stroke", "none")
      .attr("rx", 2)
      .attr("ry", 2);

    // Draw filled part based on resource usage
    if (actorResourceInfo && actorResourceInfo.usage > 0) {
      sectionGroup
        .append("rect")
        .attr("x", 0)
        .attr("y", padding)
        .attr("width", actorWidth * actorResourceInfo.usage)
        .attr("height", actorHeight)
        .attr("fill", getActorColor(actor))
        .attr("opacity", searchTerm && !actorMatchesSearchFn(actor) ? 0.3 : 0.7)
        .attr("stroke", "none")
        .attr("rx", 2)
        .attr("ry", 2);

      // Add dashed line divider at the boundary
      sectionGroup
        .append("line")
        .attr("x1", actorWidth * actorResourceInfo.usage)
        .attr("y1", padding)
        .attr("x2", actorWidth * actorResourceInfo.usage)
        .attr("y2", padding + actorHeight)
        .attr("stroke", "#666")
        .attr("stroke-width", 1)
        .attr("stroke-dasharray", "3,2");
    }

    // Create a clipping path for text
    const clipId = `free-actor-text-clip-${actorIndex}`;
    sectionGroup
      .append("clipPath")
      .attr("id", clipId)
      .append("rect")
      .attr("width", actorWidth - 4) // Slight padding
      .attr("height", actorHeight);

    // Add actor label with clipping
    sectionGroup
      .append("text")
      .attr("x", actorWidth / 2)
      .attr("y", actorHeight / 2 + padding)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .attr("font-size", "10px")
      .attr("fill", "#000000")
      .attr("clip-path", `url(#${clipId})`)
      .text(getActorContextValue(actor, selectedContext))
      .append("title") // Add tooltip for full context value
      .text(getActorContextValue(actor, selectedContext));

    // Add search highlight border if actor matches search
    if (actorMatchesSearchFn(actor)) {
      sectionGroup
        .append("rect")
        .attr("x", 0)
        .attr("y", padding)
        .attr("width", actorWidth)
        .attr("height", actorHeight)
        .attr("fill", "none")
        .attr("stroke", "#4caf50")
        .attr("stroke-width", 2)
        .attr("stroke-dasharray", "4,2")
        .attr("class", "search-highlight")
        .style("pointer-events", "none")
        .attr("rx", 2)
        .attr("ry", 2);
    }

    // Update currentX to include margin for next actor
    currentX += actorWidth + actorMargin;
  });
};

// Add a new helper function to get resource usage from resource_usage field
const getResourceUsageFromField = (actor: Actor, resourceType: string): any => {
  if (!actor.resourceUsage) {
    return null;
  }

  // Check if the requested resource exists in the usage data
  if (!actor.resourceUsage[resourceType]) {
    return null;
  }

  const resourceData = actor.resourceUsage[resourceType];
  const usageValue = resourceData.used;
  const baseResource = resourceData.base;
  let total = 0;

  // Get total based on the base field
  if (
    baseResource.toLowerCase() === "gpu" &&
    actor.gpuDevices &&
    actor.gpuDevices.length > 0
  ) {
    // Use GPU memory as total
    total = actor.gpuDevices[0].memoryTotal;
  } else if (baseResource.toLowerCase() === "cpu") {
    // Use 100 as total for CPU percentage
    total = 100;
  } else if (
    baseResource.toLowerCase() === "memory" &&
    actor.nodeMem &&
    actor.nodeMem.length >= 1
  ) {
    // Use node memory as total
    total = actor.nodeMem[0];
  } else if (baseResource.toLowerCase() === resourceType.toLowerCase()) {
    // If base is the same as resource type, assume usage is a percentage
    total = 100;
  } else {
    // Default case: assume usage is absolute and use 100 as total
    total = 100;
  }

  if (total === 0) {
    return null;
  }

  // Calculate usage percentage and cap at 100%
  const usagePercentage = Math.min(usageValue / total, 1);

  return {
    available: Math.max(total - usageValue, 0),
    total: total,
    used: usageValue,
    usage: usagePercentage,
  };
};

export default PhysicalVisualization;
