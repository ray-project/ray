export type Event = {
  eventId: string;
  jobId: string;
  nodeId: string;
  sourceType: string;
  sourceHostname: string;
  hostName: string;
  sourcePid: number;
  pid: number;
  label: string;
  message: string;
  timestamp: number;
  timeStamp: number;
  jobName: string;
  severity: string;
  customFields: {
    [key: string]: any;
  };
};

export type EventRsp = {
  result: boolean;
  msg: string;
  data: {
    jobId: string;
    events: Event[];
  };
};

export type EventGlobalRsp = {
  result: boolean;
  msg: string;
  data: {
    events: {
      global: Event[];
    };
  };
};
