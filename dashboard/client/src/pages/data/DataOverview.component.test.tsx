import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import DataOverview from "./DataOverview";

describe("DataOverview", () => {
  it("renders table with dataset metrics", async () => {
    const datasets = [
      {
        dataset: "test_ds1",
        state: "RUNNING",
        progress: 50,
        total: 100,
        start_time: 0,
        end_time: undefined,
        ray_data_output_bytes: {
          max: 10,
        },
        ray_data_spilled_bytes: {
          max: 20,
        },
        ray_data_current_bytes: {
          value: 30,
          max: 40,
        },
        operators: [
          {
            operator: "test_ds1_op1",
            state: "RUNNING",
            progress: 99,
            total: 101,
            ray_data_output_bytes: {
              max: 11,
            },
            ray_data_spilled_bytes: {
              max: 21,
            },
            ray_data_current_bytes: {
              value: 31,
              max: 41,
            },
          },
        ],
      },
      {
        dataset: "test_ds2",
        state: "FINISHED",
        progress: 200,
        total: 200,
        start_time: 1,
        end_time: 2,
        ray_data_output_bytes: {
          max: 50,
        },
        ray_data_spilled_bytes: {
          max: 60,
        },
        ray_data_current_bytes: {
          value: 70,
          max: 80,
        },
        operators: [],
      },
    ];
    const user = userEvent.setup();

    render(<DataOverview datasets={datasets} />, { wrapper: TEST_APP_WRAPPER });

    // First Dataset
    expect(screen.getByText("test_ds1")).toBeVisible();
    expect(screen.getByText("Total: 100")).toBeVisible();
    expect(screen.getByText("Finished: 50")).toBeVisible();
    expect(screen.getByText("Running: 50")).toBeVisible();
    expect(screen.getByText("1969/12/31 16:00:00")).toBeVisible();
    expect(screen.getByText("10.0000B")).toBeVisible();
    expect(screen.getByText("20.0000B")).toBeVisible();
    expect(screen.getByText("30.0000B/40.0000B")).toBeVisible();

    // Operator dropdown
    expect(screen.queryByText("test_ds1_op1")).toBeNull();
    await user.click(screen.getByTitle("Expand Dataset test_ds1"));
    expect(screen.getByText("test_ds1_op1")).toBeVisible();
    await user.click(screen.getByTitle("Collapse Dataset test_ds1"));
    expect(screen.queryByText("test_ds1_op1")).toBeNull();

    // Second Dataset
    expect(screen.getByText("test_ds2")).toBeVisible();
    expect(screen.getByText("Total: 200")).toBeVisible();
    expect(screen.getByText("Finished: 200")).toBeVisible();
    expect(screen.getByText("1969/12/31 16:00:01")).toBeVisible();
    expect(screen.getByText("1969/12/31 16:00:02")).toBeVisible();
    expect(screen.getByText("50.0000B")).toBeVisible();
    expect(screen.getByText("60.0000B")).toBeVisible();
    expect(screen.getByText("70.0000B/80.0000B")).toBeVisible();
  });
});
