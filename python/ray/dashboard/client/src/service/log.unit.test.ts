import { getStateApiDownloadLogUrl, MAX_LINES_FOR_LOGS } from "./log";

describe("getStateApiDownloadLogUrl", () => {
  it("only uses parameters provided but doesn't fetch when parameters are null", () => {
    expect.assertions(9);

    expect(
      getStateApiDownloadLogUrl({
        nodeId: "node-id",
        filename: "file.log",
      }),
    ).toStrictEqual(
      `api/v0/logs/file?node_id=node-id&filename=file.log&lines=${MAX_LINES_FOR_LOGS}`,
    );

    expect(
      getStateApiDownloadLogUrl({
        nodeId: "node-id",
        filename: "file.log",
      }),
    ).toStrictEqual(
      `api/v0/logs/file?node_id=node-id&filename=file.log&lines=${MAX_LINES_FOR_LOGS}`,
    );

    expect(
      getStateApiDownloadLogUrl({
        taskId: "task-id",
        suffix: "err",
      }),
    ).toStrictEqual(
      `api/v0/logs/file?task_id=task-id&suffix=err&lines=${MAX_LINES_FOR_LOGS}`,
    );

    expect(
      getStateApiDownloadLogUrl({
        taskId: "task-id",
        suffix: "out",
      }),
    ).toStrictEqual(
      `api/v0/logs/file?task_id=task-id&suffix=out&lines=${MAX_LINES_FOR_LOGS}`,
    );

    expect(
      getStateApiDownloadLogUrl({
        actorId: "actor-id",
        suffix: "err",
      }),
    ).toStrictEqual(
      `api/v0/logs/file?actor_id=actor-id&suffix=err&lines=${MAX_LINES_FOR_LOGS}`,
    );

    expect(
      getStateApiDownloadLogUrl({
        nodeId: null,
        filename: "file.log",
      }),
    ).toBeNull();

    expect(
      getStateApiDownloadLogUrl({
        nodeId: null,
        filename: null,
      }),
    ).toBeNull();

    expect(
      getStateApiDownloadLogUrl({
        taskId: null,
        suffix: "err",
      }),
    ).toBeNull();

    expect(
      getStateApiDownloadLogUrl({
        actorId: null,
        suffix: "err",
      }),
    ).toBeNull();
  });
});
