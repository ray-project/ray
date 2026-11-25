# Trivy image scans

This package scans Ray's container images for known vulnerabilities with
[Trivy](https://trivy.dev), and reports their CRITICAL and HIGH findings.

- **In CI**, release-automation postmerge builds scan the images they
  publish. The steps are in `.buildkite/image-scan.rayci.yml` and are off
  until `RAYCI_IMAGE_SCAN=1` is set. See [How CI runs the scans](#how-ci-runs-the-scans).
- **On your machine**, `local_scan_images` scans any image you name, such as a
  published `rayproject/ray` tag or any other image, and writes the same
  report as CI. Most of this page is about this tool.

## Scan images locally

```bash
bazel run //ci/ray_ci/automation/trivy:local_scan_images -- \
    --image rayproject/ray:2.60.0-py312-cpu \
    --image ubuntu:22.04 \
    --work-dir ~/.cache/ray-trivy \
    --output-dir /tmp/trivy-report
```

Then read `/tmp/trivy-report/report.md`. The run also writes
`annotation.md`, `summary.json`, `vulns.csv` and `results/` there.
`--help` lists every option.

### What you need

- **Linux x86_64 with Docker**, and permission to run `docker` without
  `sudo`. The pinned Trivy and crane binaries are Linux x86_64, and Trivy
  runs in a container with no network access. macOS isn't supported.
- **Bazel**, as for the rest of the repo. `bazel run` builds the tool with
  the pinned Trivy.
- **Network access** to the images' registries, and to
  `public.ecr.aws/aquasecurity`, which publishes the vulnerability DBs.
- **Disk space:**
  - about 1.1 GB for the DBs, kept in `--work-dir`;
  - temporary space there for the unpacked DBs and the pulled images,
    several GB for a Ray image. Without `--work-dir`, all of it goes in the
    system temp directory (`$TMPDIR`, often `/tmp`), which is a small,
    memory-backed filesystem on some distros;
  - room in Docker's data directory for the volumes the scan uses.

A run takes about 2 minutes for small images. A Ray image takes about 12,
most of it pulling the image.

### Choose the images

Pass each image with `--image`, as often as you like, or list them in a
file with `--images-file`. You can use both; the file's images come after
the `--image` ones.

```text
# images.txt: one image per line; "#" starts a comment.
rayproject/ray:2.60.0-py312-cpu
rayproject/ray:2.60.0-py312-cu128   # the CUDA variant
ubuntu:22.04
```

```bash
bazel run //ci/ray_ci/automation/trivy:local_scan_images -- \
    --images-file images.txt --work-dir ~/.cache/ray-trivy --output-dir /tmp/trivy-report
```

- **Names:** name each image with a tag (`repo:tag`), a digest
  (`repo@sha256:…`) or both. Without either, it means `:latest`.
  - Only the characters image names, tags and digests use are accepted:
    letters, digits, `.`, `_`, `-`, `/`, `:` and `@`. It must start with a
    letter or digit.
  - Each image may appear only once.
- **From a registry:** images are pulled from their registry. To scan one
  you built locally, push it to a registry first; see
  [Scan an image you built](#scan-an-image-you-built).
- **Architecture:** `--architecture` (default `x86_64`, or `aarch64`) picks
  which architecture's image to scan from a multi-platform tag. Most public tags are
  multi-platform. A single-platform image must match it, or it fails with
  an error such as `… is arm64, expected amd64`.
- **Digests:** each image is scanned by digest, so the report says exactly
  what was scanned even if the tag moves later. Images are resolved
  before the DBs are prepared, so if none resolves, such as a single
  mistyped tag, the run stops without downloading anything.
- **Relative paths** in options are relative to where you started
  `bazel run`, and `~` works.

### Reuse the DBs, and keep them fresh

Trivy needs two DBs, its vulnerability DB and its Java DB, over 1 GB
together. Downloading them every run wastes bandwidth and disk, so keep
them:

- **Keep them:** pass `--work-dir DIR`. The first run downloads the DBs
  into `DIR`, and every later run with the same `--work-dir` reuses them.
  Pick a directory outside the repo, such as `~/.cache/ray-trivy`, and use
  the same one every time.
- **Warnings:** a run that downloads the DBs without reusing kept ones says
  so with a `WARNING`, whether there's no `--work-dir` or the directory has
  no DBs yet. The warning is a reminder to reuse them, not an error.
- **Reuse:** the log names the kept DBs' date and age:

  ```text
  INFO: Reusing the Trivy DBs in /home/me/.cache/ray-trivy (DB created 2026-01-01, 2 days ago); pass --refresh-db to download fresh ones.
  ```

- **Refresh:** pass `--refresh-db`, with `--work-dir`, to replace them. The
  new DBs download beside the kept ones and replace them only once
  complete, so a failed refresh keeps the old ones.
- **Share:** several runs may use the same `--work-dir` at once. Runs
  reusing the DBs scan side by side; a run downloading or refreshing them
  waits for those, and they for it, logging
  `Waiting for another local_scan_images run using DIR`.

A kept DB grows stale. The upstream DB gets new advisories often, so an
old copy can miss vulnerabilities published since it was built. Refresh:

- before you share results or base a release decision on them;
- when the reuse message shows the DB is more than a day or so old. Over
  7 days old, the message becomes a `WARNING`, and the report notes the
  DB's age;
- when you're checking whether a newly announced CVE affects an image.

Reuse it freely while you iterate, for example while you work out an
ignore list.

The report's first line always gives the DB's date, for example
`DB 2026-01-01 (sha256:0123456789ab)`, so a report says what it was
checked against.

Each run also checks the kept DBs against the digests the upstream
registry recorded for them. If that check fails, for instance because
the old version isn't served any more, every image fails with the error,
and the report suggests running again with `--refresh-db`.

### Ignore lists

An ignore list accepts specific findings, so the report shows them as
suppressed instead of open. Pass one with `--ignorefile PATH`. It applies
to every image in the run, and without one nothing is ignored. It must be
Trivy's YAML format, in a `.yaml` or `.yml` file; the plain `.trivyignore`
format (one ID per line) is refused.

- **Reuse Ray's lists.** CI keeps one list per image family in
  [`ignore/`](ignore/):
  `ray`, `ray-extra`, `ray-llm` and `ray-llm-extra`. To see a published Ray
  image as CI does, pass its family's list:

  ```bash
  bazel run //ci/ray_ci/automation/trivy:local_scan_images -- \
      --image rayproject/ray:2.60.0-py312-cpu \
      --ignorefile ci/ray_ci/automation/trivy/ignore/ray.trivyignore.yaml \
      --work-dir ~/.cache/ray-trivy --output-dir /tmp/trivy-report
  ```

- **Write your own.** It's a Trivy YAML ignore file
  ([format](https://trivy.dev/docs/latest/configuration/filtering/)):

  ```yaml
  # my-images.trivyignore.yaml
  vulnerabilities:
    - id: CVE-2026-00001
      # Scope the entry, so it can't hide the same CVE elsewhere.
      purls:
        - pkg:deb/ubuntu/libexample@1.0.0-1
      statement: "Accepted until the base image ships libexample 1.0.1; our images don't use the affected feature."
      expired_at: 2026-12-31
  ```

  - **Scope it:** narrow each entry with `paths` (file paths in the image)
    or `purls` (package URLs) where you can. A purl without qualifiers
    (`?arch=…&distro=…`) matches the package whatever they are. The raw
    results in `results/` show each package's full purl.
  - **Say why:** give each entry a `statement`, which the report shows next
    to the finding.
  - **Let it expire:** give each entry an `expired_at` a few weeks out.
    Trivy stops applying an expired entry, so the finding comes back to be
    checked again.

Keep a personal list outside the repo. The lists in `ignore/` are applied
by CI, so a change to them goes through review like any code change.

### Scan an image you built

`local_scan_images` pulls images from a registry; it can't read the
images in your local Docker. To scan one you built, push it to a registry
first. A throwaway registry on `localhost` will do:

```bash
docker run -d --rm --name local-registry -p 127.0.0.1:5000:5000 registry:2
docker build -t localhost:5000/my-image:dev .
docker push localhost:5000/my-image:dev
bazel run //ci/ray_ci/automation/trivy:local_scan_images -- \
    --image localhost:5000/my-image:dev \
    --work-dir ~/.cache/ray-trivy --output-dir /tmp/trivy-report
docker stop local-registry   # with --rm, this also deletes what was pushed
```

- **No TLS needed on `localhost`:** crane uses plain HTTP for `localhost`,
  loopback addresses and private-network IPs. A registry under any other
  hostname needs HTTPS.
- **Keep it private:** binding to `127.0.0.1` keeps the registry, which has
  no authentication, off the network.
- **Digests differ:** the report gives the pushed image's digest, not the
  image ID `docker images` shows; it's the same content.
- **Architecture:** `docker build` makes an image for your machine's
  architecture, so keep `--architecture` at its default on x86_64.

### Private registries and ECR

crane pulls the images with the credentials in `~/.docker/config.json`.

- **Docker Hub and most registries:** run `docker login` first if the
  images are private, or if you hit Docker Hub's limits on anonymous pulls
  (`toomanyrequests`).
- **ECR in `us-west-2`** (`<account>.dkr.ecr.us-west-2.amazonaws.com`):
  `local_scan_images` logs in itself, with your AWS credentials, so you don't need
  to run `aws ecr get-login-password`. Set your profile first:

  ```bash
  aws sso login --profile <profile>
  AWS_PROFILE=<profile> bazel run //ci/ray_ci/automation/trivy:local_scan_images -- \
      --image <account>.dkr.ecr.us-west-2.amazonaws.com/<repo>:<tag> \
      --work-dir ~/.cache/ray-trivy --output-dir /tmp/trivy-report
  ```

  `docker login` stores the token, valid for 12 hours, unencrypted in
  `~/.docker/config.json`, as Docker warns. Run `docker logout <registry>`
  when you're done if you'd rather not keep it.
- **If the login fails**, for example after your AWS session expired, the
  run logs `Could not log in to <registry>: …` and carries on: that
  registry's images fail to pull, each with crane's error, and the others
  still scan.
- **ECR in other regions** isn't supported yet: the login always gets its
  token from `us-west-2`, so it fails as above.
- **Public ECR** (`public.ecr.aws`) needs no login.

## Read the results

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Every image was scanned, and none has a fixable CRITICAL or HIGH finding. |
| 10 | Every image was scanned, and at least one has a fixable CRITICAL or HIGH finding. |
| 11 | At least one image wasn't scanned. This takes precedence over 10. |
| 1 or 2 | The run itself failed, e.g. bad options (2) or the DBs couldn't be downloaded (1). |

### The files in `--output-dir`

| File | What it holds |
|---|---|
| `report.md` | The full report, described below. |
| `annotation.md` | The same report sized for a Buildkite annotation (under 1 MiB): shorter CVE lists and fewer rows when it would be too long. For a few images, it's the same as `report.md`. |
| `summary.json` | The verdict, the totals and each image's counts and findings, for scripts and comparing runs. |
| `vulns.csv` | One row per image and CRITICAL or HIGH finding, suppressed ones included. |
| `results/<name>.json.gz` | Trivy's raw JSON for the image: every severity and every package. |
| `results/<name>.status.json` | The image's name, digest, scan status and the DBs it was checked against. |

`<name>` is the image's name and tag or digest, with every character other than letters,
digits, `.`, `_` and `-` turned into `_`. A run first removes its own
images' files from an earlier run in the same `--output-dir`, and leaves
anything else there.

### The report

Only CRITICAL and HIGH findings count. Lower severities are only in the raw
results. Findings are grouped across images by CVE, package and installed
version.

- **The first line** gives the counts, how many images were scanned, the
  Trivy version and the DB's date:

  ```text
  **12 fixable CRITICAL/HIGH** · 34 with no fix yet · 2 suppressed · 3/3 images scanned · Trivy 0.76.1 · DB 2026-01-01 (`sha256:0123456789ab`)
  ```

  (Example values.)

- **Incomplete** lists each image that wasn't scanned, with the reason,
  and any other problem, such as unreadable DB metadata. See
  [Troubleshooting](#troubleshooting).
- **Fixable CVEs:** one row per finding that has a fix. A finding is
  fixable when Trivy names a fixed version, unless its status is
  `affected`, `will_not_fix`, `fix_deferred` or `end_of_life`. The columns:
  - **Installed → Fixed:** the version to upgrade to.
  - **Where:** where the fix goes.
    - `os` is a distro package: update it with the image's package manager.
    - A `site-packages` or `dist-packages` directory is a Python
      environment: pip.
    - Anything else is the file itself, such as a jar or a binary, even one
      inside a wheel: it has to be rebuilt.
  - **Images:** which images have it. When more than one image was
    scanned and all of them have it, it says "all N scanned images".
- **Fixable Images:** the same findings, one row per image, most affected
  first.
- **No Fix Yet and No Fix Yet Per Image:** findings with no fix available,
  by package and by image. A package with more than 10 such CVEs in an
  image shows a count; `vulns.csv` lists them all. These need a decision,
  not an upgrade: accept the risk with an ignore entry, or drop the
  package.
- **Suppressed:** findings your ignore list accepted, with each entry's
  statement. It appears only when there are some.
- **Per Image:** each image's digest, package count, and its fixable,
  no-fix and suppressed counts.
- **The last line** names the architecture scanned, such as
  `linux/amd64`, and where the raw results are. It also notes an image
  with no OS packages, and a reused DB over 7 days old.

### summary.json and vulns.csv

`summary.json` (schema version 1) holds:
- `exit_code`;
- `scanner`: the Trivy version and the DBs' digests and dates;
- `issues`;
- `totals`: images, scanned, fixable, no_fix, suppressed;
- an entry per image, with its `ref`, `digest`, `scanned`, `error`,
  `packages`, `counts` (fixable and no-fix CRITICAL and HIGH, and
  suppressed), and `findings` as `CVE|package|installed` keys.

To see what changed between two runs, compare their `findings` keys, or
their `vulns.csv` files. Locally, the `build` fields and the Ray fields
(`python_version`, `platform`, `published_tags`) are empty, and
`image_type` is `image`.

`vulns.csv` has the columns `image`, `digest`, `vuln_id`, `severity`,
`fixable`, `status`, `suppressed`, `pkg_class`, `pkg_type`, `pkg_name`,
`installed`, `fixed`, `location` and `path`, plus the build and Ray fields
above.

### Troubleshooting

| Message | Meaning and fix |
|---|---|
| `WARNING: Downloading the Trivy DBs … for this run only` | No `--work-dir`. Add one to keep the DBs. |
| `WARNING: Reusing the Trivy DBs … CVEs published since then are missing` | The kept DB is over 7 days old. Pass `--refresh-db`. |
| `Could not download the Trivy DBs: …` | `public.ecr.aws` wasn't reachable, or refused the request. The kept DBs, if any, are unchanged. |
| `Could not log in to <registry>: …` | The ECR login failed: an expired AWS session, or a registry outside `us-west-2`. That registry's images fail; the others still scan. |
| `No image could be resolved, so nothing was downloaded.` | Every image failed to resolve; the report gives each one's error. |
| `Waiting for another local_scan_images run using DIR` | Another run is downloading DBs into the same `--work-dir`, or scanning with them while this one wants to download. It continues once that's done. |
| Every image: `group failed before scanning: …` | Something every image needs failed: Docker isn't running or you can't use it, or the kept DBs failed their upstream check (the report then suggests `--refresh-db`). The message names the cause. |
| `… is arm64, expected amd64` | A single-platform image of the other architecture. Pass `--architecture aarch64`. |
| `no OS packages detected (OS: not detected); checked its other packages` | Trivy found no distro packages, e.g. in an image built `FROM scratch`. Its other packages, such as a Go binary's modules, are still checked and reported, and the report notes it. An image with no packages at all counts as not scanned, so a scan that saw nothing never looks clean. |
| `… isn't a Trivy YAML ignore file (.yaml or .yml)` | `--ignorefile` must use Trivy's YAML format. |
| `'…' is not an image name` | The name has a character image names don't use, or starts with `-`. |
| `… is given twice` / `… would share the result file …` | Two images are the same, or map to the same `<name>`. |
| `toomanyrequests` | Docker Hub's anonymous pull limit. Run `docker login`. |

## Update the Trivy version pin

Every scan, in CI and with `local_scan_images`, runs the Trivy release pinned in
[`pin/trivy_versions.bzl`](pin/trivy_versions.bzl): `TRIVY_VERSION` and
`TRIVY_SHA256`, the sha256 of `trivy_<version>_Linux-64bit.tar.gz`.
`TRIVY_SIGNER_IDENTITY` follows from the version. `WORKSPACE` loads the
file to download that release.

Never update it by tag alone. Trivy v0.69.4, and the v0.69.5 and v0.69.6
Docker Hub images, were malicious
([GHSA-69fq-xp46-6x23](https://github.com/advisories/GHSA-69fq-xp46-6x23)),
and Trivy's own pipeline built and signed v0.69.4, so a valid signature
alone proves nothing. The steps below are the checklist in the file's
docstring, which stays the reference. Record each step's output in the
bump's PR description.

1. **Pick a release at least 14 days old**, and check that neither Trivy's
   [security advisories](https://github.com/aquasecurity/trivy/security/advisories)
   nor [OSV](https://osv.dev) list a problem with it.
2. **Check that the tag's commit is on `main` or its release branch.** For
   `main` or `release/v<major>.<minor>`, this must print `behind` or
   `identical`:

   ```bash
   gh api repos/aquasecurity/trivy/compare/<branch>...v<version> --jq .status
   ```

3. **Verify the signature on the release's checksums file.** Download the
   checksums file, its sigstore bundle and the Linux tarball, then check
   that the file was signed by the expected identity. A different identity
   stops the bump until someone finds out why.

   ```bash
   gh release download v<version> --repo aquasecurity/trivy \
       --pattern 'trivy_<version>_checksums.txt*' \
       --pattern 'trivy_<version>_Linux-64bit.tar.gz'
   cosign verify-blob \
       --bundle trivy_<version>_checksums.txt.sigstore.json \
       --certificate-oidc-issuer https://token.actions.githubusercontent.com \
       --certificate-identity https://github.com/aquasecurity/trivy/.github/workflows/reusable-release.yaml@refs/tags/v<version> \
       trivy_<version>_checksums.txt
   ```

4. **Update the pin.** Set `TRIVY_VERSION`, and copy the tarball's sha256
   from that verified checksums file into `TRIVY_SHA256`.
5. **Check the binary was built from the tag.** `go version -m` on the
   extracted `trivy` shows the tag's commit as `vcs.revision`. Ignore
   `+dirty`: about half of Trivy's releases are built dirty.
6. **Test it, and compare scans.**
   - Run `bazel test //ci/ray_ci/automation/trivy:all`.
   - Compare a scan by the new binary with one by the old: for example,
     scan the same images with `local_scan_images` before and after the bump.
     Use the same `--work-dir` without `--refresh-db`, so only the binary
     differs.
   - Explain any change in the counts or the findings, from `summary.json`.

A change in `pin/` also invalidates the build caches of the ray-core,
ray-cpp-core and ray-java images, whose build contexts copy that
directory, so expect slower CI on the bump's PR.

## How CI runs the scans

The same modules run in release-automation postmerge builds, when
`RAYCI_IMAGE_SCAN=1` is set:

1. `trivy_db` stages the DBs once per build, pinned by digest, in the
   build's S3 folder, which [`scan_store.py`](scan_store.py) names.
2. Each scan step runs `scan_images` on one architecture and platform group
   of the images the build published, and uploads the results to that
   folder.
3. `build_report` runs after every scan step, whatever they did. It checks
   the results against the images the pipeline asks for, and writes the
   report to the folder and to the job's artifacts. `.buildkite/hooks/post-command`
   posts it as the build's `trivy-scan` annotation.

Every step soft-fails, so a scan never blocks a release. The modules'
docstrings have the details:

| Module | Role |
|---|---|
| [`local_scan_images.py`](local_scan_images.py) | The local scan. |
| [`scan_images.py`](scan_images.py) | Resolves the images, pulls them and runs the contained Trivy. |
| [`trivy_db.py`](trivy_db.py) | Stages the pinned DBs. |
| [`scan_store.py`](scan_store.py) | The build's S3 folder, or a local one. |
| [`trivy_report.py`](trivy_report.py) | Reads results, decides the verdict, renders the report. |
| [`build_report.py`](build_report.py) | CI's report step. |
| [`pin/trivy_versions.bzl`](pin/trivy_versions.bzl) | The Trivy version pin, and the checklist for bumping it. |
