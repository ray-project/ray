import { filterRuntimeEnvSystemVariables } from "./util";

describe("filterRuntimeEnvSystemVariables", () => {
  it("filters out system variables", () => {
    expect(
      filterRuntimeEnvSystemVariables({
        pip: {
          pip_check: true,
          packages: ["chess", "foo", "bar"],
          pip_version: "1.2.3",
        },
        env_vars: {
          FOO: "foo",
          BAR: "5",
        },
        working_dir: ".",
        _ray_release: "2.3.1",
        _ray_commit: "12345abc",
        _inject_current_ray: false,
      }),
    ).toEqual({
      pip: {
        pip_check: true,
        packages: ["chess", "foo", "bar"],
        pip_version: "1.2.3",
      },
      env_vars: {
        FOO: "foo",
        BAR: "5",
      },
      working_dir: ".",
    });
  });
});
