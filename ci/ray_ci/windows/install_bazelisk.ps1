$Env:BAZELISK_URL="https://github.com/bazelbuild/bazelisk/releases/download/v1.20.0/bazelisk-windows-amd64.exe"
Remove-Item -LiteralPath C:\bazel -Force -Recurse
New-Item -ItemType Directory -Path C:\bazel -Force | Out-Null;
Invoke-WebRequest -Uri $env:BAZELISK_URL -OutFile C:\bazel\bazel.exe;
