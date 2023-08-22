export type Event = {
  eventId: string;
  source_type: SourceType;
  message: string;
  timestamp: number;
  severity: SeverityLevel;
  custom_fields: {
    [key: string]: any;
  };
};

export type EventRsp = {
  result: boolean;
  msg: string;
  data: {
    result: Event[];
  };
};

export type Align = "inherit" | "left" | "center" | "right" | "justify";

export type Filters = {
  sourceType: string[]; // TODO: Chao, multi-select severity level in filters button is a P1
  severityLevel: string[]; // TODO: Chao, multi-select severity level in filters button is a P1
  entityName: string | undefined;
  entityId: string | undefined;
};
