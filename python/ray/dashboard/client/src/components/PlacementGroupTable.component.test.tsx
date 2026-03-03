import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { PlacementGroup, PlacementGroupState } from "../type/placementGroup";
import { TEST_APP_WRAPPER } from "../util/test-utils";
import PlacementGroupTable from "./PlacementGroupTable";

const MOCK_PLACEMENT_GROUPS: PlacementGroup[] = [
  {
    placement_group_id: "pg-123456789",
    name: "MyPlacementGroup1",
    creator_job_id: "job-987654321",
    state: PlacementGroupState.CREATED,
    stats: {
      scheduling_state: "SUCCESS",
    },
    bundles: [
      {
        bundle_id: "bundle-1",
        node_id: "node-1",
        unit_resources: {
          cpu: 4,
          memory: 8192,
        },
        label_selector: {
          "test-label-key": "test-label-value",
        },
      },
      {
        bundle_id: "bundle-2",
        node_id: null,
        unit_resources: {
          cpu: 2,
          memory: 4096,
        },
        label_selector: null,
      },
    ],
  },
  {
    placement_group_id: "pg-987654321",
    name: "MyPlacementGroup2",
    creator_job_id: "job-123456789",
    state: PlacementGroupState.PENDING,
    stats: {
      scheduling_state: "PENDING",
    },
    bundles: [
      {
        bundle_id: "bundle-3",
        node_id: "node-2",
        unit_resources: {
          cpu: 8,
          memory: 16384,
          gpu: 1,
        },
        label_selector: {
          "gpu-required": "true",
        },
      },
    ],
  },
  {
    placement_group_id: "pg-555666777",
    name: "MyPlacementGroup3",
    creator_job_id: "job-987654321",
    state: PlacementGroupState.REMOVED,
    stats: null,
    bundles: [
      {
        bundle_id: "bundle-4",
        node_id: null,
        unit_resources: {},
        label_selector: {},
      },
    ],
  },
];

// These tests are slow because they involve a lot of interactivity.
// Clicking various buttons and waiting for the table to update.
// So we increase the timeout to 40 seconds.
jest.setTimeout(40000);

describe("PlacementGroupTable", () => {
  it("renders a table of placement groups with all columns", () => {
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Check that all column headers are present
    const idHeaders = screen.getAllByText("ID");
    expect(idHeaders.length).toBeGreaterThan(0);

    const nameHeaders = screen.getAllByText("Name");
    expect(nameHeaders.length).toBeGreaterThan(0);

    const jobIdHeaders = screen.getAllByText("Job Id");
    expect(jobIdHeaders.length).toBeGreaterThan(0);

    const stateHeaders = screen.getAllByText("State");
    expect(stateHeaders.length).toBeGreaterThan(0);

    const reservedResourcesHeaders = screen.getAllByText("Reserved Resources");
    expect(reservedResourcesHeaders.length).toBeGreaterThan(0);

    const labelSelectorHeaders = screen.getAllByText("Label Selector");
    expect(labelSelectorHeaders.length).toBeGreaterThan(0);

    const schedulingDetailHeaders = screen.getAllByText("Scheduling Detail");
    expect(schedulingDetailHeaders.length).toBeGreaterThan(0);

    // Check that placement group data is displayed
    expect(screen.getByText("pg-123456789")).toBeInTheDocument();
    expect(screen.getByText("MyPlacementGroup1")).toBeInTheDocument();
    const jobIdElements = screen.getAllByText("job-987654321");
    expect(jobIdElements.length).toBeGreaterThan(0);
    expect(screen.getByText("SUCCESS")).toBeInTheDocument();
  });

  it("renders placement groups filtered by placement group ID", async () => {
    const user = userEvent.setup();
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Get the input directly by its label
    const input = screen.getByLabelText("Placement group ID");

    // Filter by placement group ID
    await user.type(input, "pg-123456789");

    // Wait for the filter to be applied
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Check that only the filtered placement group is shown
    const pg123Elements = screen.getAllByText("pg-123456789");
    expect(pg123Elements.length).toBeGreaterThan(0);

    // Check that other placement groups are not shown
    expect(screen.queryByText("pg-987654321")).not.toBeInTheDocument();
    expect(screen.queryByText("pg-555666777")).not.toBeInTheDocument();
  });

  it("renders placement groups filtered by state", async () => {
    const user = userEvent.setup();
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Get the input directly by its label
    const input = screen.getByLabelText("State");

    // Filter by state
    await user.type(input, "CREATED");

    // Wait for the filter to be applied
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Check that only the filtered placement group is shown
    expect(screen.queryByText("pg-123456789")).toBeInTheDocument();
    expect(screen.queryByText("pg-987654321")).not.toBeInTheDocument();
    expect(screen.queryByText("pg-555666777")).not.toBeInTheDocument();
  });

  it("renders placement groups filtered by job ID", async () => {
    const user = userEvent.setup();
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Get the input directly by its label
    const input = screen.getByLabelText("Job Id");

    // Filter by job ID
    await user.type(input, "job-987654321");

    // Wait for the filter to be applied
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Check that only the filtered placement groups are shown
    expect(screen.queryByText("pg-123456789")).toBeInTheDocument();
    expect(screen.queryByText("pg-987654321")).not.toBeInTheDocument();
    expect(screen.queryByText("pg-555666777")).toBeInTheDocument();
  });

  it("renders placement groups filtered by name", async () => {
    const user = userEvent.setup();
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Get the input directly by its label
    const input = screen.getByLabelText("Name");

    // Filter by name
    await user.type(input, "MyPlacementGroup1");

    // Wait for the filter to be applied
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Check that only the filtered placement group is shown
    const nameElements = screen.getAllByText("MyPlacementGroup1");
    expect(nameElements.length).toBeGreaterThan(0);

    // Check that other placement groups are not shown
    expect(screen.queryByText("MyPlacementGroup2")).not.toBeInTheDocument();
    expect(screen.queryByText("MyPlacementGroup3")).not.toBeInTheDocument();
  });

  it("renders placement groups with pagination", async () => {
    const user = userEvent.setup();
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Check that pagination controls are present
    expect(screen.getByRole("navigation")).toBeInTheDocument();

    // Change page size
    const pageSizeInput = screen.getByLabelText("Page Size");
    await user.clear(pageSizeInput);
    await user.type(pageSizeInput, "2");

    // Verify pagination works
    expect(screen.getByText("pg-123456789")).toBeInTheDocument();
    expect(screen.getByText("pg-987654321")).toBeInTheDocument();
    expect(screen.queryByText("pg-555666777")).not.toBeInTheDocument();
  });

  it("renders placement groups with job ID prop", () => {
    render(
      <PlacementGroupTable
        placementGroups={MOCK_PLACEMENT_GROUPS}
        jobId="job-987654321"
      />,
      {
        wrapper: TEST_APP_WRAPPER,
      },
    );

    // Check that the job ID filter is pre-populated
    const jobIdFilter = screen.getByLabelText("Job Id");
    expect(jobIdFilter).toHaveValue("job-987654321");
  });

  it("renders placement groups with empty bundles", () => {
    const placementGroupsWithEmptyBundles = [
      {
        ...MOCK_PLACEMENT_GROUPS[0],
        bundles: [],
      },
    ];

    render(
      <PlacementGroupTable placementGroups={placementGroupsWithEmptyBundles} />,
      {
        wrapper: TEST_APP_WRAPPER,
      },
    );

    // Check that empty bundles are handled gracefully
    expect(screen.getByText("pg-123456789")).toBeInTheDocument();
    // Check that empty resources are handled - might be rendered as "[]" or not at all
    const emptyResourceElements = screen.getAllByText("[]");
    expect(emptyResourceElements.length).toBeGreaterThan(0);
  });

  it("renders placement groups with null stats", () => {
    const placementGroupsWithNullStats = [
      {
        ...MOCK_PLACEMENT_GROUPS[0],
        stats: null,
      },
    ];

    render(
      <PlacementGroupTable placementGroups={placementGroupsWithNullStats} />,
      {
        wrapper: TEST_APP_WRAPPER,
      },
    );

    // Check that null stats are handled gracefully
    expect(screen.getByText("pg-123456789")).toBeInTheDocument();
    expect(screen.getByText("-")).toBeInTheDocument(); // Null scheduling detail
  });

  it("renders placement groups with empty name", () => {
    const placementGroupsWithEmptyName = [
      {
        ...MOCK_PLACEMENT_GROUPS[0],
        name: "",
      },
    ];

    render(
      <PlacementGroupTable placementGroups={placementGroupsWithEmptyName} />,
      {
        wrapper: TEST_APP_WRAPPER,
      },
    );

    // Check that empty names are handled gracefully
    expect(screen.getByText("pg-123456789")).toBeInTheDocument();
    expect(screen.getByText("-")).toBeInTheDocument(); // Empty name
  });

  it("renders state counter for placement groups", () => {
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Check that state counter is present by looking for the total count
    expect(screen.getByText(/x 3/)).toBeInTheDocument(); // Total count of 3 placement groups
  });

  it("renders resource requirements as JSON dialog", () => {
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Check that resource requirements are rendered as dialog buttons
    // Look for the button text or check that the table cell contains resource data
    const resourceCells = screen.getAllByText(/cpu|memory|gpu/i);
    expect(resourceCells.length).toBeGreaterThan(0);
  });

  it("renders label selector as JSON dialog", () => {
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Check that label selector is rendered as dialog buttons
    // Look for the button text or check that the table cell contains label data
    const labelCells = screen.getAllByText(/test-label-key|gpu-required/i);
    expect(labelCells.length).toBeGreaterThan(0);
  });

  it("handles placement groups with different states", () => {
    render(<PlacementGroupTable placementGroups={MOCK_PLACEMENT_GROUPS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Check that different states are displayed by looking for the placement group rows
    expect(screen.getByText("pg-123456789")).toBeInTheDocument();
    expect(screen.getByText("pg-987654321")).toBeInTheDocument();
    expect(screen.getByText("pg-555666777")).toBeInTheDocument();

    // Check that the table contains the expected states (using getAllByText to handle multiple instances)
    const createdElements = screen.getAllByText("CREATED");
    const pendingElements = screen.getAllByText("PENDING");
    const removedElements = screen.getAllByText("REMOVED");

    expect(createdElements.length).toBeGreaterThan(0);
    expect(pendingElements.length).toBeGreaterThan(0);
    expect(removedElements.length).toBeGreaterThan(0);
  });

  it("renders empty table when no placement groups provided", () => {
    render(<PlacementGroupTable placementGroups={[]} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Check that column headers are still present by looking for table headers specifically
    const tableHeaders = screen.getAllByText("ID");
    expect(tableHeaders.length).toBeGreaterThan(0);

    const nameHeaders = screen.getAllByText("Name");
    expect(nameHeaders.length).toBeGreaterThan(0);

    const jobIdHeaders = screen.getAllByText("Job Id");
    expect(jobIdHeaders.length).toBeGreaterThan(0);

    const stateHeaders = screen.getAllByText("State");
    expect(stateHeaders.length).toBeGreaterThan(0);

    const reservedResourcesHeaders = screen.getAllByText("Reserved Resources");
    expect(reservedResourcesHeaders.length).toBeGreaterThan(0);

    const labelSelectorHeaders = screen.getAllByText("Label Selector");
    expect(labelSelectorHeaders.length).toBeGreaterThan(0);

    const schedulingDetailHeaders = screen.getAllByText("Scheduling Detail");
    expect(schedulingDetailHeaders.length).toBeGreaterThan(0);

    // Check that no data rows are present
    expect(screen.queryByText("pg-123456789")).not.toBeInTheDocument();
  });
});
