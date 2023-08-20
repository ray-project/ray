import { SeverityLevel, SourceType } from "../components/NewEventTable";

export type Event = {
  eventId: string;
  sourceType: SourceType;
  sourceHostname: string;
  hostName: string;
  sourcePid: number;
  pid: number;
  message: string;
  timestamp: number;
  severity: SeverityLevel;
  customFields: {
    [key: string]: any;
  };
};

export type EventRsp = {
  result: boolean;
  msg: string;
  data: {
    result: {
      total: number;
      result: Event[];
    };
  };
};
