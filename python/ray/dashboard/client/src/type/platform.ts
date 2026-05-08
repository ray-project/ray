export type PlatformEvent = {
  sourceEventId: string;
  severity: "INFO" | "WARNING" | "ERROR";
  source: {
    platform: string;
    component: string;
    metadata: { [key: string]: string };
  };
  objectKind: string;
  objectName: string;
  message: string;
  reason: string;
  timestampNs: number;
};

export type PlatformEventsRsp = {
  result: boolean;
  msg: string;
  data: {
    events: PlatformEvent[];
  };
};
