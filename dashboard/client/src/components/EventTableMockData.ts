// eslint-disable-next-line prefer-arrow/prefer-arrow-functions

const SEVERITIES = ["INFO", "DEBUG"];
const SOURCES = ["Serve", "GCS", "AUTOSCALER"];
const MESSAGES = [
  "Serve App {app name} started.",
  "Serve App {app name} deleted.",
  "Deployment {deployment name} created",
  "Deployment {deployment name} is running",
  "Deployment {deployment name} deleted.",
  "Serve Deployment “job” started.",
  "Replica {replica name} started",
  "Replica {replica name} died",
  "Ray Serve server is running.",
  "Ray Serve server is terminated.",
  "Ray Serve HTTP Proxy started on node {node id}, {node ip}",
  "Ray Serve HTTP Proxy removed from node {node id}, {node ip}",
];

const CUSTOM_MESSAGES = [
  "Terminating instance i-xxx due to compute config live update.",
];

function generateRandomEvent() {
  const severity = SEVERITIES[Math.floor(Math.random() * SEVERITIES.length)];
  const source = SOURCES[Math.floor(Math.random() * SOURCES.length)];
  const message =
    source === "AUTOSCALER"
      ? CUSTOM_MESSAGES[0]
      : MESSAGES[Math.floor(Math.random() * MESSAGES.length)];
  const customFields = {} as {
    [key: string]: any;
  };

  if (message.includes("{app name}")) {
    const appName = "my-app-" + Math.floor(Math.random() * 100);
    customFields.app_name = appName;
  }

  if (message.includes("{deployment name}")) {
    const deploymentName = "deployment-" + Math.floor(Math.random() * 100);
    customFields.deployment_name = deploymentName;
  }

  if (message.includes("{replica name}")) {
    const replicaName = "replica-" + Math.floor(Math.random() * 100);
    customFields.replica_name = replicaName;
  }

  if (message.includes("{node id}") && message.includes("{node ip}")) {
    const nodeId = "node-" + Math.floor(Math.random() * 100);
    const nodeIp = "192.168.1." + Math.floor(Math.random() * 256);
    customFields.node_id = nodeId;
    customFields.node_ip = nodeIp;
  }

  return {
    eventId: Math.random().toString(36).substring(7),
    sourceType: source,
    hostName: "po-dev.inc.alipay.net",
    pid: Math.floor(Math.random() * 65536),
    label: "",
    message: message,
    timestamp: Date.now() / 1000,
    severity: severity,
    customFields: customFields,
  };
}

const generatedEvents = [];
const autoscalerEvent = generateRandomEvent(); // Generate the custom AUTOSCALER event first
generatedEvents.push(autoscalerEvent);

for (let i = 1; i < 20; i++) {
  // Start from 1 to skip the autoscaler event
  generatedEvents.push(generateRandomEvent());
}

export const MOCK_DATA = {
  result: true,
  msg: "All events fetched.",
  data: {
    events: {
      "64000000": generatedEvents,
    },
  },
};
