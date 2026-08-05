import { render, screen, waitFor } from "@testing-library/react";
import React from "react";
import "@testing-library/jest-dom";
import { _resetRuntimeEnvRedactionCache } from "../../common/RuntimeEnvRedaction";
import { get } from "../../service/requestHandlers";
import { UnifiedJob } from "../../type/job";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { JobMetadataSection } from "./JobDetailInfoPage";

jest.mock("../../service/requestHandlers");

const mockedGet = jest.mocked(get);

const JOB = {
  job_id: "01000000",
  submission_id: null,
  type: "DRIVER",
  status: "RUNNING",
  entrypoint: "python script.py",
  runtime_env: {
    env_vars: { MY_SECRET: "<redacted>" },
  },
} as unknown as UnifiedJob;

const mockRedactionEnabled = (redactionEnabled: boolean) => {
  mockedGet.mockImplementation((url: string) => {
    if (url === "/api/v0/runtime_env_redaction") {
      return Promise.resolve({ data: { data: { redactionEnabled } } }) as any;
    }
    return Promise.resolve({ data: { data: {} } }) as any;
  });
};

describe("JobMetadataSection runtime environment redaction help", () => {
  beforeEach(() => {
    mockedGet.mockReset();
    _resetRuntimeEnvRedactionCache();
  });

  it("shows the redaction help icon when redaction is enabled", async () => {
    mockRedactionEnabled(true);

    render(<JobMetadataSection job={JOB} />, { wrapper: TEST_APP_WRAPPER });

    await waitFor(() => {
      expect(
        screen.getByLabelText(/RAY_DASHBOARD_REDACT_RUNTIME_ENV=0/),
      ).toBeInTheDocument();
    });
  });

  it("does not show the help icon when redaction is disabled", async () => {
    mockRedactionEnabled(false);

    render(<JobMetadataSection job={JOB} />, { wrapper: TEST_APP_WRAPPER });

    await waitFor(() => {
      expect(mockedGet).toHaveBeenCalledWith("/api/v0/runtime_env_redaction");
    });
    expect(
      screen.queryByLabelText(/RAY_DASHBOARD_REDACT_RUNTIME_ENV=0/),
    ).not.toBeInTheDocument();
  });

  it("does not show the help icon when the job has no env_vars", async () => {
    mockRedactionEnabled(true);
    const jobWithoutEnvVars = {
      ...JOB,
      runtime_env: { working_dir: "gcs://pkg.zip" },
    } as unknown as UnifiedJob;

    render(<JobMetadataSection job={jobWithoutEnvVars} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await waitFor(() => {
      expect(mockedGet).toHaveBeenCalledWith("/api/v0/runtime_env_redaction");
    });
    expect(
      screen.queryByLabelText(/RAY_DASHBOARD_REDACT_RUNTIME_ENV=0/),
    ).not.toBeInTheDocument();
  });
});
