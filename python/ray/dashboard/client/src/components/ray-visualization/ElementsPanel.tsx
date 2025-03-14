import React, { useCallback, useEffect, useState } from "react";
import "./ElementsPanel.css";

type GraphData = {
  actors: Array<{
    id: string;
    name: string;
    language: string;
    devices?: string[];
  }>;
  methods: Array<{
    id: string;
    name: string;
    actorId: string;
    language: string;
  }>;
  functions: Array<{
    id: string;
    name: string;
    language: string;
  }>;
};

type ElementsPanelProps = {
  onElementSelect: (element: any) => void;
  selectedElementId: string | null;
  graphData: GraphData;
  onSearchChange?: (searchTerm: string) => void;
};

const ElementsPanel = ({
  onElementSelect,
  selectedElementId,
  graphData,
  onSearchChange,
}: ElementsPanelProps) => {
  const [activeTab, setActiveTab] = useState("actors");
  const [searchTerm, setSearchTerm] = useState("");
  const [expandedActors, setExpandedActors] = useState<Record<string, boolean>>(
    {},
  );

  // Notify parent component when search term changes
  useEffect(() => {
    if (onSearchChange) {
      onSearchChange(searchTerm);
    }
  }, [searchTerm, onSearchChange]);

  // Filter items based on search term
  const filterItems = useCallback(
    (items: any[]) => {
      if (!searchTerm) {
        return items;
      }
      return items.filter(
        (item) =>
          item.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
          item.id.toLowerCase().includes(searchTerm.toLowerCase()),
      );
    },
    [searchTerm],
  );

  // Get methods for a specific actor
  const getActorMethods = useCallback(
    (actorId: string) => {
      const methods = graphData.methods.filter(
        (method) => method.actorId === actorId,
      );
      if (!searchTerm) {
        return methods;
      }

      return methods.filter(
        (method) =>
          method.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
          method.id.toLowerCase().includes(searchTerm.toLowerCase()),
      );
    },
    [graphData.methods, searchTerm],
  );

  // Filter actors and their methods based on search term
  const filterActorsAndMethods = useCallback(() => {
    if (!searchTerm) {
      return graphData.actors;
    }

    return graphData.actors.filter((actor) => {
      const actorMatches =
        actor.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        actor.id.toLowerCase().includes(searchTerm.toLowerCase());

      const actorMethods = getActorMethods(actor.id);
      const methodMatches = actorMethods.some(
        (method) =>
          method.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
          method.id.toLowerCase().includes(searchTerm.toLowerCase()),
      );

      return actorMatches || methodMatches;
    });
  }, [searchTerm, graphData.actors, getActorMethods]);

  // Update expanded actors when search term changes
  useEffect(() => {
    const actorsToExpand: Record<string, boolean> = {};
    graphData.actors.forEach((actor) => {
      const methods = getActorMethods(actor.id);
      // Only expand if there's a search term and methods exist
      actorsToExpand[actor.id] = searchTerm !== "" && methods.length > 0;
    });
    setExpandedActors(actorsToExpand);
  }, [searchTerm, getActorMethods, graphData.actors]);

  const filteredActors = filterActorsAndMethods();
  const filteredFunctions = filterItems(graphData.functions);

  // Toggle expanded state for an actor
  const toggleActorExpand = (actorId: string) => {
    setExpandedActors((prev) => ({
      ...prev,
      [actorId]: !prev[actorId],
    }));
  };

  return (
    <div className="elements-panel">
      <div className="elements-header">
        <h3>Instances</h3>
        <div className="search-container">
          <input
            type="text"
            placeholder="Search..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="search-input"
          />
        </div>
      </div>

      <div className="tab-container">
        <div
          className={`tab ${activeTab === "actors" ? "active" : ""}`}
          onClick={() => setActiveTab("actors")}
        >
          Actors ({graphData.actors.length})
        </div>
        <div
          className={`tab ${activeTab === "functions" ? "active" : ""}`}
          onClick={() => setActiveTab("functions")}
        >
          Functions ({graphData.functions.length})
        </div>
      </div>

      <div className="elements-table-container">
        {activeTab === "actors" && (
          <table className="elements-table">
            <thead>
              <tr>
                <th>Name</th>
              </tr>
            </thead>
            <tbody>
              {filteredActors.map((actor) => (
                <React.Fragment key={actor.id}>
                  <tr
                    className={`actor-row ${
                      actor.id === selectedElementId ? "selected" : ""
                    }`}
                  >
                    <td>
                      <button
                        className={`expand-button ${
                          expandedActors[actor.id] ? "expanded" : ""
                        }`}
                        onClick={() => toggleActorExpand(actor.id)}
                      >
                        {expandedActors[actor.id] ? "−" : "+"}
                      </button>
                      <span
                        onClick={() =>
                          onElementSelect({ ...actor, type: "actor" })
                        }
                      >
                        {actor.name}
                      </span>
                    </td>
                  </tr>
                  {expandedActors[actor.id] && (
                    <tr className="methods-container">
                      <td>
                        <div className="actor-methods">
                          <table className="methods-table">
                            <tbody>
                              {getActorMethods(actor.id).map((method) => (
                                <tr
                                  key={method.id}
                                  className={
                                    method.id === selectedElementId
                                      ? "selected"
                                      : ""
                                  }
                                  onClick={() =>
                                    onElementSelect({
                                      ...method,
                                      type: "method",
                                    })
                                  }
                                >
                                  <td>{method.name}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        )}

        {activeTab === "functions" && (
          <table className="elements-table">
            <thead>
              <tr>
                <th>Name</th>
              </tr>
            </thead>
            <tbody>
              {filteredFunctions.map((func) => (
                <tr
                  key={func.id}
                  className={func.id === selectedElementId ? "selected" : ""}
                  onClick={() => onElementSelect({ ...func, type: "function" })}
                >
                  <td>{func.name}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

export default ElementsPanel;
