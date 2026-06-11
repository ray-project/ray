export type PlatformEventSource = {
  platform: string;
  component: string;
  metadata: { [key: string]: string };
};

export type PlatformEventData = {
  source?: PlatformEventSource;
  objectKind: string;
  objectName: string;
  message: string;
  reason: string;
  customFields?: { [key: string]: string };
};

export type PlatformEvent = {
  eventId: string;
  sourceType: string;
  eventType: string;
  timestamp: string;
  severity: string;
  message: string;
  platformEvent: PlatformEventData;
};

export type PlatformEventsRsp = {
  result: boolean;
  msg: string;
  data: {
    events: PlatformEvent[];
  };
};
