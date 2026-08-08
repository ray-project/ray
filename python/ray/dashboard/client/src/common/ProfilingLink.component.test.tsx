import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import "@testing-library/jest-dom";
import { get } from "../service/requestHandlers";
import { TEST_APP_WRAPPER } from "../util/test-utils";
import {
  _resetProfilingEnabledCache,
  CpuProfilingLink,
  CpuStackTraceLink,
  DEFAULT_PROFILING_DEFAULTS,
  ProfilerButton,
  ProfilingDefaults,
  TaskCpuStackTraceLink,
} from "./ProfilingLink";

jest.mock("../service/requestHandlers");

const mockedGet = jest.mocked(get);

// The link components (unlike ProfilerButton) request /api/profiling_enabled to
// decide whether profiling is on and to seed their dialog defaults. Mock the
// shared get() helper so those tests are deterministic, and clear
// ProfilingLink's module-level cache so each test controls the response
// independently.
const mockProfiling = (
  enabled: boolean,
  defaults: Partial<ProfilingDefaults> = {},
) => {
  _resetProfilingEnabledCache();
  mockedGet.mockResolvedValue({
    data: {
      data: {
        profilingEnabled: enabled,
        profilingDefaults: defaults,
      },
    },
  } as any);
};

afterEach(() => {
  jest.restoreAllMocks();
  mockedGet.mockReset();
  _resetProfilingEnabledCache();
});

describe("fetchProfilingEnabled", () => {
  beforeEach(() => {
    mockedGet.mockReset();
    _resetProfilingEnabledCache();
  });

  it("calls get() with /api/profiling_enabled instead of raw fetch", async () => {
    mockedGet.mockResolvedValueOnce({
      data: { data: { profilingEnabled: false } },
    } as any);

    render(<CpuProfilingLink pid={12345} nodeId="node-abc" type="" />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await waitFor(() => {
      expect(mockedGet).toHaveBeenCalledWith("/api/profiling_enabled");
    });
  });
});

describe("ProfilerButton", () => {
  const mockProps = {
    profilerUrl: "http://localhost:3000/worker/memory_profile",
  };
  it("renders button correctly", () => {
    render(<ProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    const button = screen.getByLabelText(/Memory Profiling/);
    expect(button).toBeInTheDocument();
  });

  it("opens the dialog when the button is clicked", async () => {
    const user = userEvent.setup();
    render(<ProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    const button = screen.getByLabelText(/Memory Profiling/);

    user.click(button);

    // check all components exist in dialog
    await waitFor(() => {
      const dialogTitle = screen.getByText("Memory Profiling Config");
      expect(dialogTitle).toBeInTheDocument();
      const reportButton = screen.getByText(/Generate report/);
      expect(reportButton).toBeInTheDocument();
      const durationInput = screen.getByLabelText(/Duration/);
      expect(durationInput).toBeInTheDocument();
      const leaksCheckbox = screen.getByText(/Leaks/);
      expect(leaksCheckbox).toBeInTheDocument();
      const nativeCheckbox = screen.getByText(/Native/);
      expect(nativeCheckbox).toBeInTheDocument();
      const allocatorCheckbox = screen.getByText(/Python Allocator Tracing/);
      expect(allocatorCheckbox).toBeInTheDocument();
    });
  });

  it("closes the dialog when the cancel button is clicked", async () => {
    const user = userEvent.setup();
    render(<ProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    const button = screen.getByLabelText(/Memory Profiling/);

    await user.click(button);

    const cancelButton = screen.getByRole("button", { name: /Cancel/ });
    await user.click(cancelButton);

    await waitFor(() => {
      const dialogTitle = screen.queryByText(/Memory Profiling Config/);
      expect(dialogTitle).not.toBeInTheDocument();
    });
  });

  it("builds the memory profiling URL from the default params", async () => {
    const user = userEvent.setup();
    // No `defaults` prop -> falls back to DEFAULT_PROFILING_DEFAULTS
    // (memoryDuration=10, leaks off, native off, allocators off, flamegraph).
    render(<ProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    const button = screen.getByLabelText(/Memory Profiling/);
    await user.click(button);

    const formatSelect = screen.getByLabelText(/flamegraph/);
    expect(formatSelect).toBeInTheDocument();
    expect(screen.getByText(/Generate report/)).toHaveAttribute(
      "href",
      `${mockProps.profilerUrl}&format=flamegraph&duration=${DEFAULT_PROFILING_DEFAULTS.memoryDuration}` +
        `&leaks=0&native=0&trace_python_allocators=0`,
    );
  });

  it("seeds the dialog from the provided defaults", async () => {
    const user = userEvent.setup();
    render(
      <ProfilerButton
        {...mockProps}
        defaults={{
          ...DEFAULT_PROFILING_DEFAULTS,
          native: true,
          leaks: false,
          memoryDuration: 30,
          memoryFormat: "table",
        }}
      />,
      { wrapper: TEST_APP_WRAPPER },
    );
    await user.click(screen.getByLabelText(/Memory Profiling/));

    expect(screen.getByText(/Generate report/)).toHaveAttribute(
      "href",
      `${mockProps.profilerUrl}&format=table&duration=30` +
        `&leaks=0&native=1&trace_python_allocators=0`,
    );
  });
});

describe("CpuStackTraceLink (worker)", () => {
  const props = { pid: 1234, nodeId: "node-abc", type: "" };

  it("does not send a hardcoded native=0 and reflects the default", async () => {
    mockProfiling(true, { native: true });
    const user = userEvent.setup();
    render(<CpuStackTraceLink {...props} />, { wrapper: TEST_APP_WRAPPER });

    // Wait for the profiling-enabled fetch to flip the link on, then open the
    // dialog. The trigger is the anchor labelled with the dialog title.
    const trigger = await screen.findByLabelText(/Stack Trace Config/);
    await user.click(trigger);

    const link = await screen.findByText(/Get stack trace/);
    const href = link.getAttribute("href");
    // Native default is true here, so it must serialize as native=1 -- and there
    // must never be a stray hardcoded native=0.
    expect(href).toBe(
      `worker/traceback?pid=1234&node_id=node-abc&native=1&subprocesses=0`,
    );
    expect(href).not.toContain("native=0");
  });

  it("disables and forces off Native when py-spy native is unsupported", async () => {
    // Non-Linux dashboard: the native default is on, but py-spy drops --native
    // off-Linux, so the checkbox is disabled and the URL must not request native.
    mockProfiling(true, { native: true, pyspyNativeSupported: false });
    const user = userEvent.setup();
    render(<CpuStackTraceLink {...props} />, { wrapper: TEST_APP_WRAPPER });

    await user.click(await screen.findByLabelText(/Stack Trace Config/));

    expect(screen.getByRole("checkbox", { name: /Native/ })).toBeDisabled();
    const href = (await screen.findByText(/Get stack trace/)).getAttribute(
      "href",
    );
    expect(href).toBe(
      `worker/traceback?pid=1234&node_id=node-abc&native=0&subprocesses=0`,
    );
  });

  it("shows a disabled label when profiling is off", async () => {
    mockProfiling(false);
    render(<CpuStackTraceLink {...props} />, { wrapper: TEST_APP_WRAPPER });
    // Disabled label renders as plain (non-link) text.
    expect(await screen.findByText(/Stack Trace/)).toBeInTheDocument();
    expect(screen.queryByText(/Get stack trace/)).not.toBeInTheDocument();
  });
});

describe("CpuProfilingLink (worker)", () => {
  it("builds a cpu_profile URL with duration, format, and flags", async () => {
    mockProfiling(true, {
      cpuDuration: 7,
      cpuFormat: "speedscope",
      idle: true,
    });
    const user = userEvent.setup();
    render(<CpuProfilingLink pid={99} nodeId="n1" type="" />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await user.click(await screen.findByLabelText(/CPU Profiling Config/));
    const link = await screen.findByText(/Generate report/);
    expect(link.getAttribute("href")).toBe(
      `worker/cpu_profile?pid=99&node_id=n1&duration=7&format=speedscope` +
        `&native=0&idle=1&subprocesses=0`,
    );
  });

  it("disables submit and shows an error for an out-of-range duration", async () => {
    // Seed an invalid duration (below the min of 1) so the dialog opens invalid.
    mockProfiling(true, { cpuDuration: 0 });
    const user = userEvent.setup();
    render(<CpuProfilingLink pid={99} nodeId="n1" type="" />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await user.click(await screen.findByLabelText(/CPU Profiling Config/));
    // Error helper text is shown.
    expect(
      await screen.findByText(
        `Duration must be between 1 and ${DEFAULT_PROFILING_DEFAULTS.maxDuration}`,
      ),
    ).toBeInTheDocument();
    // The submit button (rendered as an anchor) is disabled.
    const submit = screen.getByText(/Generate report/);
    expect(submit).toHaveAttribute("aria-disabled", "true");
  });
});

describe("TaskCpuStackTraceLink", () => {
  it("builds a task/traceback URL and reflects defaults", async () => {
    mockProfiling(true, { subprocesses: true });
    const user = userEvent.setup();
    render(
      <TaskCpuStackTraceLink taskId="t1" attemptNumber={0} nodeId="n2" />,
      { wrapper: TEST_APP_WRAPPER },
    );

    await user.click(await screen.findByLabelText(/Stack Trace Config/));
    const link = await screen.findByText(/Get stack trace/);
    expect(link.getAttribute("href")).toBe(
      `task/traceback?task_id=t1&attempt_number=0&node_id=n2` +
        `&native=0&subprocesses=1`,
    );
  });
});
