export type K8sEvent = {
  eventId: string;
  severity: "INFO" | "WARNING" | "ERROR";
  message: string;
  reason: string;
  timestampMs: number;
  relatedNodeId: string;
  customFields: { [key: string]: string };
};

export type K8sEventsRsp = {
  result: boolean;
  msg: string;
  data: {
    events: K8sEvent[];
  };
};
