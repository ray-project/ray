nodeMap.forEach((node) => {
  const updateChildren = (nodeData: FlameNode): FlameNode => {
    const realnode = nodeMap.get(nodeData.name);
    const children = [];
    if (realnode) {
      for (const child of realnode.children || []) {
        children.push(updateChildren(child));
      }
    }
    nodeData.children = children;
    return {
      name: nodeData.name,
      customValue: nodeData.customValue,
      originalValue: nodeData.originalValue,
      count: nodeData.count,
      children: nodeData.children ? [...nodeData.children] : [],
      actorName: nodeData.actorName,
      hide: nodeData.hide,
      fade: nodeData.fade,
      highlight: nodeData.highlight,
      dimmed: nodeData.dimmed,
      value: nodeData.value,
      delta: nodeData.delta,
      totalInParent: nodeData.totalInParent,
      extras: nodeData.extras ? { ...nodeData.extras } : undefined,
    };
  };
  updateChildren(node);
});

// Replace with a more robust implementation
// Create a helper function to deep copy nodes
const processedNodes = new Map<string, FlameNode>();

const createDeepCopy = (node: FlameNode): FlameNode => {
  // If we've already processed this node, return the deep copy we made
  const existingCopy = processedNodes.get(node.name);
  if (existingCopy) {
    return existingCopy;
  }
  
  // Create a new node with all properties copied
  const copy: FlameNode = {
    name: node.name,
    customValue: node.customValue,
    originalValue: node.originalValue,
    count: node.count,
    children: [], // We'll fill this in a moment
    actorName: node.actorName,
    hide: node.hide,
    fade: node.fade,
    highlight: node.highlight,
    dimmed: node.dimmed,
    value: node.value,
    delta: node.delta,
    totalInParent: node.totalInParent ? [...node.totalInParent] : undefined,
    extras: node.extras ? { ...node.extras } : undefined,
  };
  
  // Store in our map to avoid circular references
  processedNodes.set(node.name, copy);
  
  // Process children recursively
  if (node.children && node.children.length > 0) {
    copy.children = node.children.map(child => createDeepCopy(child));
  }
  
  return copy;
};

// Apply deep copy to all nodes in the nodeMap
nodeMap.forEach((node, key) => {
  nodeMap.set(key, createDeepCopy(node));
}); 