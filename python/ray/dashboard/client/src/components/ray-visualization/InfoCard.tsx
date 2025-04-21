import React, { useEffect } from "react";
import "./InfoCard.css";

// Define types for the graph data structures
type BaseNode = {
  id: string;
  name: string;
  language: string;
  type: string;
};

type Actor = BaseNode & {
  type: "actor";
  devices: string[];
  state?: string;
  pid?: number;
  required_resources?: Record<string, number>;
  gpuDevices?: Array<{
    index: number;
    name: string;
    uuid: string;
    memoryUsed: number;
    memoryTotal: number;
    utilization?: number;
  }>;
};

type Method = BaseNode & {
  type: "method";
  actorId: string;
  actorName?: string;
};

type FunctionNode = BaseNode & {
  type: "function";
};

type CallFlow = {
  source: string;
  target: string;
  count: number;
  startTime: number;
};

type DataFlow = {
  source: string;
  target: string;
  speed?: string;
  argpos?: number;
  duration?: number;
  size?: number;
  timestamp?: number;
};

type GraphData = {
  actors: Actor[];
  methods: Method[];
  functions: FunctionNode[];
  callFlows: CallFlow[];
  dataFlows: DataFlow[];
};

type NodeWithCount = BaseNode & {
  type: "actor" | "method" | "function";
  count?: number;
  devices?: string[];
  actorId?: string;
  actorName?: string;
};

type NodeWithSpeed = BaseNode & {
  type: "actor" | "method" | "function";
  speed?: string;
  argpos?: number;
  duration?: number;
  size?: number;
  devices?: string[];
  actorId?: string;
  actorName?: string;
};

type FoldedSections = {
  Methods: boolean;
  Devices: boolean;
  Callers: boolean;
  Callees: boolean;
  "Data Dependencies": boolean;
};

type InfoCardProps = {
  data: Node | null;
  visible: boolean;
  graphData: GraphData;
  currentView?: "logical" | "physical" | "flame" | "call_stack" | "analysis";
  onNavigateToLogicalView?: (nodeId: string) => void;
};

type Node = Actor | Method | FunctionNode;

// Helper functions to find connected nodes
const findCallInputs = (
  nodeId: string,
  graphData: GraphData,
): NodeWithCount[] => {
  return graphData.callFlows
    .filter((flow) => flow.target === nodeId)
    .map((flow) => {
      const sourceNode = findNodeById(flow.source, graphData);
      return {
        ...sourceNode,
        count: flow.count,
      } as NodeWithCount;
    });
};

const findDataInputs = (
  nodeId: string,
  graphData: GraphData,
): NodeWithSpeed[] => {
  return graphData.dataFlows
    .filter((flow) => flow.target === nodeId)
    .map((flow) => {
      const sourceNode = findNodeById(flow.source, graphData);
      return {
        ...sourceNode,
        speed: flow.speed,
        argpos: flow.argpos,
        duration: flow.duration,
        size: flow.size,
      } as NodeWithSpeed;
    });
};

const findCallOutputs = (
  nodeId: string,
  graphData: GraphData,
): NodeWithCount[] => {
  return graphData.callFlows
    .filter((flow) => flow.source === nodeId)
    .map((flow) => {
      const targetNode = findNodeById(flow.target, graphData);
      return {
        ...targetNode,
        count: flow.count,
      } as NodeWithCount;
    });
};

const findDataOutputs = (
  nodeId: string,
  graphData: GraphData,
): NodeWithSpeed[] => {
  return graphData.dataFlows
    .filter((flow) => flow.source === nodeId)
    .map((flow) => {
      const targetNode = findNodeById(flow.target, graphData);
      return {
        ...targetNode,
        speed: flow.speed,
        argpos: flow.argpos,
        duration: flow.duration,
        size: flow.size,
      } as NodeWithSpeed;
    });
};

// Find a node by ID across all node types
const findNodeById = (id: string, graphData: GraphData): Node => {
  const actor = graphData.actors.find((actor) => actor.id === id);
  if (actor) {
    return { ...actor, type: "actor" };
  }

  const method = graphData.methods.find((method) => method.id === id);
  if (method) {
    const actor = graphData.actors.find((a) => a.id === method.actorId);
    return {
      ...method,
      type: "method",
      actorName: actor ? actor.name : "Unknown Actor",
    };
  }

  const func = graphData.functions.find((func) => func.id === id);
  if (func) {
    return { ...func, type: "function" };
  }

  return { id, name: id, type: "function", language: "unknown" };
};

// Get all methods for an actor
const getActorMethods = (actorId: string, graphData: GraphData): Method[] => {
  return graphData.methods
    .filter((method) => method.actorId === actorId)
    .map((method) => ({ ...method, type: "method" as const }));
};

// Aggregated connections for an actor (include all methods)
const getActorConnections = (actorId: string, graphData: GraphData) => {
  const methods = getActorMethods(actorId, graphData);
  const methodIds = methods.map((method) => method.id);

  const callInputs: NodeWithCount[] = [];
  const dataInputs: NodeWithSpeed[] = [];
  const callOutputs: NodeWithCount[] = [];
  const dataOutputs: NodeWithSpeed[] = [];

  // Process each method's connections
  methodIds.forEach((methodId) => {
    callInputs.push(...findCallInputs(methodId, graphData));
    dataInputs.push(...findDataInputs(methodId, graphData));
    callOutputs.push(...findCallOutputs(methodId, graphData));
    dataOutputs.push(...findDataOutputs(methodId, graphData));
  });

  // Remove duplicates by ID
  const uniqueCallInputs = Array.from(
    new Map(callInputs.map((item) => [item.id, item])).values(),
  );
  const uniqueDataInputs = Array.from(
    new Map(dataInputs.map((item) => [item.id, item])).values(),
  );
  const uniqueCallOutputs = Array.from(
    new Map(callOutputs.map((item) => [item.id, item])).values(),
  );
  const uniqueDataOutputs = Array.from(
    new Map(dataOutputs.map((item) => [item.id, item])).values(),
  );

  return {
    callInputs: uniqueCallInputs,
    dataInputs: uniqueDataInputs,
    callOutputs: uniqueCallOutputs,
    dataOutputs: uniqueDataOutputs,
    methods,
  };
};

const InfoCard = ({
  data,
  visible,
  graphData,
  currentView = "logical",
  onNavigateToLogicalView,
}: InfoCardProps) => {
  // Add debugging
  useEffect(() => {
    console.log("InfoCard rendering with data:", data);
  }, [data]);

  type SectionKey = keyof FoldedSections;

  // Initialize all sections as folded
  const [foldedSections, setFoldedSections] = React.useState<FoldedSections>({
    Methods: true,
    Devices: true,
    Callers: true,
    Callees: true,
    "Data Dependencies": true,
  });

  // Toggle section fold
  const toggleSection = (title: SectionKey) => {
    setFoldedSections((prev) => ({
      ...prev,
      [title]: !prev[title],
    }));
  };

  // Render devices section
  const renderDevicesSection = (
    devices: string[],
    gpuDevices?: Actor["gpuDevices"],
  ) => {
    const hasDevices =
      (devices && devices.length > 0) || (gpuDevices && gpuDevices.length > 0);

    if (!hasDevices) {
      return (
        <div className="connection-section">
          <div
            className="connection-header"
            onClick={() => toggleSection("Devices")}
          >
            <div className="connection-header-left">
              <span className="fold-icon">
                {foldedSections["Devices"] ? "▶" : "▼"}
              </span>
              <h4>Devices</h4>
            </div>
            <span className="connection-count-badge">(0)</span>
          </div>
          <p className="empty-connection">None</p>
        </div>
      );
    }

    // Function to get color for memory usage
    const getMemoryUsageColor = (usage: number) => {
      if (usage < 0.5) {
        return "#4caf50";
      } // Green for low usage
      if (usage < 0.8) {
        return "#ff9800";
      } // Orange for medium usage
      return "#f44336"; // Red for high usage
    };

    return (
      <div className="connection-section">
        <div
          className="connection-header"
          onClick={() => toggleSection("Devices")}
        >
          <div className="connection-header-left">
            <span className="fold-icon">
              {foldedSections["Devices"] ? "▶" : "▼"}
            </span>
            <h4>Devices</h4>
          </div>
          <span className="connection-count-badge">
            ({(devices?.length || 0) + (gpuDevices?.length || 0)})
          </span>
        </div>
        {!foldedSections["Devices"] && (
          <div className="device-info-container">
            {devices && devices.length > 0 && (
              <div className="device-section">
                <h5>General Devices</h5>
                <ul className="connection-list">
                  {devices.map((device, index) => (
                    <li key={index} className="connection-item">
                      <div className="connection-main-info">
                        <div>
                          <span className="connection-name">{device}</span>
                        </div>
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {gpuDevices && gpuDevices.length > 0 && (
              <div className="device-section">
                <h5>GPU Devices</h5>
                {gpuDevices.map((gpu) => {
                  const memoryUsage = gpu.memoryUsed / gpu.memoryTotal;
                  const memoryUsageColor = getMemoryUsageColor(memoryUsage);

                  return (
                    <div
                      key={gpu.uuid}
                      className="gpu-info"
                      style={{
                        border: "1px solid #e0e0e0",
                        borderRadius: "4px",
                        padding: "12px",
                        marginBottom: "12px",
                        backgroundColor: "#f8f9fa",
                      }}
                    >
                      <div className="info-row" style={{ marginBottom: "8px" }}>
                        <span
                          className="info-label"
                          style={{ fontWeight: "bold" }}
                        >
                          Device {gpu.index}:
                        </span>
                        <span className="info-value">{gpu.name}</span>
                      </div>

                      <div className="info-row" style={{ marginBottom: "8px" }}>
                        <span className="info-label">UUID:</span>
                        <span
                          className="info-value"
                          style={{ fontSize: "0.9em", fontFamily: "monospace" }}
                        >
                          {gpu.uuid}
                        </span>
                      </div>

                      <div className="info-row" style={{ marginBottom: "8px" }}>
                        <span className="info-label">GRAM Usage:</span>
                        <span className="info-value">
                          {Math.round(gpu.memoryUsed)}MB /{" "}
                          {Math.round(gpu.memoryTotal)}MB
                        </span>
                      </div>

                      <div
                        className="memory-usage-bar"
                        style={{
                          width: "100%",
                          height: "8px",
                          backgroundColor: "#e0e0e0",
                          borderRadius: "4px",
                          overflow: "hidden",
                          marginTop: "4px",
                        }}
                      >
                        <div
                          style={{
                            width: `${memoryUsage * 100}%`,
                            height: "100%",
                            backgroundColor: memoryUsageColor,
                            transition: "width 0.3s ease",
                          }}
                        />
                      </div>

                      <div
                        className="memory-usage-text"
                        style={{
                          textAlign: "right",
                          fontSize: "0.9em",
                          color: memoryUsageColor,
                          marginTop: "4px",
                        }}
                      >
                        {Math.round(memoryUsage * 100)}% Used
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}
      </div>
    );
  };

  // Render methods section
  const renderMethodsSection = (methods: Method[]) => {
    if (!methods || methods.length === 0) {
      return (
        <div className="connection-section">
          <div
            className="connection-header"
            onClick={() => toggleSection("Methods")}
          >
            <div className="connection-header-left">
              <span className="fold-icon">
                {foldedSections["Methods"] ? "▶" : "▼"}
              </span>
              <h4>Methods</h4>
            </div>
            <span className="connection-count-badge">(0)</span>
          </div>
          <p className="empty-connection">None</p>
        </div>
      );
    }

    return (
      <div className="connection-section">
        <div
          className="connection-header"
          onClick={() => toggleSection("Methods")}
        >
          <div className="connection-header-left">
            <span className="fold-icon">
              {foldedSections["Methods"] ? "▶" : "▼"}
            </span>
            <h4>Methods</h4>
          </div>
          <span className="connection-count-badge">({methods.length})</span>
        </div>
        {!foldedSections["Methods"] && (
          <ul className="connection-list">
            {methods.map((method) => (
              <li key={method.id} className="connection-item">
                <div className="connection-main-info">
                  <div>
                    <span className="connection-name">{method.name}</span>
                  </div>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    );
  };

  // Render a list of connected nodes
  const renderConnectedNodes = (
    nodes: (NodeWithCount | NodeWithSpeed)[],
    title: SectionKey,
  ) => {
    if (!nodes || nodes.length === 0) {
      return (
        <div className="connection-section">
          <div
            className="connection-header"
            onClick={() => toggleSection(title)}
          >
            <div className="connection-header-left">
              <span className="fold-icon">
                {foldedSections[title] ? "▶" : "▼"}
              </span>
              <h4>{title}</h4>
            </div>
            <span className="connection-count-badge">(0)</span>
          </div>
          {!foldedSections[title] && <p className="empty-connection">None</p>}
        </div>
      );
    }

    return (
      <div className="connection-section">
        <div className="connection-header" onClick={() => toggleSection(title)}>
          <div className="connection-header-left">
            <span className="fold-icon">
              {foldedSections[title] ? "▶" : "▼"}
            </span>
            <h4>{title}</h4>
          </div>
          <span className="connection-count-badge">({nodes.length})</span>
        </div>
        {!foldedSections[title] && (
          <ul className="connection-list">
            {nodes.map((node, index) => (
              <li key={`${node.id}-${index}`} className="connection-item">
                {node.type === "method" && node.actorName && (
                  <div className="connection-actor-info">
                    <span className="connection-actor">
                      Actor: {node.actorName}
                    </span>
                  </div>
                )}

                <div className="connection-divider"></div>

                <div className="connection-main-info">
                  <div>
                    <span className="connection-name">{node.name}</span>
                  </div>
                  {"count" in node && node.count && (
                    <span className="connection-count">{node.count}次</span>
                  )}
                  {"speed" in node && node.speed && (
                    <span className="connection-speed">{node.speed}</span>
                  )}
                </div>

                {/* Display additional data flow information if available */}
                {"speed" in node && title === "Data Dependencies" && (
                  <div className="data-flow-details">
                    <div className="detail-row">
                      <span className="detail-label">Object Type:</span>
                      <span className="detail-value">
                        {node.argpos === undefined
                          ? "val"
                          : node.argpos === -1
                          ? "return val"
                          : node.argpos === -2
                          ? "ray.put"
                          : "argument at " + Math.floor(node.argpos / 2)}
                      </span>
                    </div>
                    {node.size !== undefined && (
                      <div className="detail-row">
                        <span className="detail-label">Size:</span>
                        <span className="detail-value">
                          {node.size.toFixed(8)} MB
                        </span>
                      </div>
                    )}
                    {node.duration !== undefined && (
                      <div className="detail-row">
                        <span className="detail-label">Duration:</span>
                        <span className="detail-value">
                          {node.duration.toFixed(5)} seconds
                        </span>
                      </div>
                    )}
                    {node.duration !== undefined && (
                      <div className="detail-row">
                        <span className="detail-label">Throughput:</span>
                        <span className="detail-value">
                          {node.size !== undefined
                            ? (node.size / node.duration).toFixed(2)
                            : "N/A"}{" "}
                          MB/s
                        </span>
                      </div>
                    )}
                  </div>
                )}

                {node.type === "actor" && (
                  <div className="connection-actor-info">
                    <span className="connection-language">
                      Language: {node.language}
                    </span>
                  </div>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    );
  };

  const renderContent = () => {
    if (!data) {
      return (
        <div className="empty-state">Select an element to view details</div>
      );
    }

    switch (data.type) {
      case "actor": {
        // Get all information for this actor including its methods
        const connections = getActorConnections(data.id, graphData);

        return (
          <React.Fragment>
            <h3>{data.name}</h3>
            <div className="info-row">
              <span className="info-label">Type:</span>
              <span className="info-value">Actor</span>
            </div>
            <div className="info-row">
              <span className="info-label">Language:</span>
              <span className="info-value">{data.language}</span>
            </div>
            <div className="info-row">
              <span className="info-label">ID:</span>
              <span className="info-value">{data.id}</span>
            </div>
            {data.state && (
              <div className="info-row">
                <span className="info-label">State:</span>
                <span className="info-value">{data.state}</span>
              </div>
            )}
            {data.pid && (
              <div className="info-row">
                <span className="info-label">PID:</span>
                <span className="info-value">{data.pid}</span>
              </div>
            )}

            {renderDevicesSection(data.devices, data.gpuDevices)}
            {renderMethodsSection(connections.methods)}

            <div className="connections-container">
              {renderConnectedNodes(connections.callInputs, "Callers")}
              {renderConnectedNodes(connections.callOutputs, "Callees")}
              {renderConnectedNodes(
                connections.dataInputs,
                "Data Dependencies",
              )}
            </div>
          </React.Fragment>
        );
      }
      case "method": {
        const callInputs = findCallInputs(data.id, graphData);
        const dataInputs = findDataInputs(data.id, graphData);
        const callOutputs = findCallOutputs(data.id, graphData);
        const actor = findNodeById(data.actorId, graphData) as Actor;

        return (
          <React.Fragment>
            <h3>{data.name === "_main" ? "main" : data.name}</h3>
            <div className="info-row">
              <span className="info-label">Type:</span>
              <span className="info-value">Method</span>
            </div>
            <div className="info-row">
              <span className="info-label">Language:</span>
              <span className="info-value">{data.language}</span>
            </div>
            <div className="info-row">
              <span className="info-label">Actor:</span>
              <span className="info-value">{data.actorName}</span>
            </div>

            {actor && renderDevicesSection(actor.devices, actor.gpuDevices)}

            <div className="connections-container">
              {renderConnectedNodes(callInputs, "Callers")}
              {renderConnectedNodes(callOutputs, "Callees")}
              {renderConnectedNodes(dataInputs, "Data Dependencies")}
            </div>
          </React.Fragment>
        );
      }
      case "function": {
        const callInputs = findCallInputs(data.id, graphData);
        const dataInputs = findDataInputs(data.id, graphData);
        const callOutputs = findCallOutputs(data.id, graphData);

        return (
          <React.Fragment>
            <h3>{data.name}</h3>
            <div className="info-row">
              <span className="info-label">Type:</span>
              <span className="info-value">Function</span>
            </div>
            <div className="info-row">
              <span className="info-label">Language:</span>
              <span className="info-value">{data.language}</span>
            </div>

            <div className="connections-container">
              {renderConnectedNodes(callInputs, "Callers")}
              {renderConnectedNodes(callOutputs, "Callees")}
              {renderConnectedNodes(dataInputs, "Data Dependencies")}
            </div>
          </React.Fragment>
        );
      }
      default:
        const unknownData = data as BaseNode;
        return (
          <div className="default-state">
            <h3>{unknownData.name}</h3>
          </div>
        );
    }
  };

  // The panel is now always visible with a fixed position
  const panelStyle = {
    position: "fixed" as const,
    top: "56px", // Align with elements table
    right: 0,
    height: "calc(100vh - 56px)", // Adjust height to account for top offset
    width: "320px",
    background: "white",
    zIndex: 9999,
    overflowY: "auto" as const,
    borderLeft: "1px solid #e1e4e8",
  };

  return (
    <div className="sidebar-panel" style={panelStyle}>
      <div className="info-panel-content">{renderContent()}</div>
    </div>
  );
};

export default InfoCard;
