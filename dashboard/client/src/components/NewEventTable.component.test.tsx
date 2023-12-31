import { render, screen } from "@testing-library/react";
import React from "react";
import { SeverityLevel } from "./event";
import NewEventTable from "./NewEventTable";
import { useEvents } from "./useEvents";

jest.mock("./useEvents", () => ({
  useEvents: jest.fn(),
}));

const MOCK_EVENTS_DATA = [
  {
    eventId: "1",
    source_type: "core_worker",
    message: "Test Message 1",
    timestamp: 1677611455,
    severity: "info",
    custom_fields: {
      field1: "value1",
    },
  },
  {
    eventId: "2",
    source_type: "raylet",
    message: "Test Message 2",
    timestamp: 1677611460,
    severity: "warning",
    custom_fields: {
      field2: "value2",
    },
  },
];

describe("NewEventTable", () => {
  beforeEach(() => {
    const mockedUseEvents = jest.mocked(useEvents);

    (mockedUseEvents as jest.Mock).mockReturnValue({
      data: MOCK_EVENTS_DATA,
      error: null,
      isLoading: false,
    });
  });

  it("renders correct table content and table head with given props", async () => {
    const props = {
      defaultSeverityLevels: ["INFO", "WARNING"] as SeverityLevel[],
      entityName: "JOB_ID",
      entityId: "010000",
    };

    // Render the component
    render(<NewEventTable {...props} />);
    await screen.findByText(/Timestamp/i);

    // Check that the correct request params are being used (as part of the mocked useEvents hook)
    expect(useEvents).toHaveBeenCalledWith(
      {
        entityId: "010000",
        entityName: "JOB_ID",
        severityLevel: ["INFO", "WARNING"],
        sourceType: [],
      },
      1, // Page number
    );

    expect(screen.getByText(/custom fields/i)).toBeInTheDocument();

    expect(screen.getByText(/core_worker/)).toBeInTheDocument();
    expect(screen.getByText(/warning/)).toBeInTheDocument();
    expect(screen.getByText(/2023-02-28 19:10:55/)).toBeInTheDocument();
  });
});
