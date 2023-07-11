$Env:BAZELISK_URL="https://github.com/bazelbuild/bazelisk/releases/download/v1.16.0/bazelisk-windows-amd64.exe"
Write-Host ('Downloading {0} ...' -f $env:BAZELISK_URL);
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
New-Item -ItemType Directory -Path C:\bazel -Force | Out-Null;
Invoke-WebRequest -Uri $env:BAZELISK_URL -OutFile 'C:\bazel\bazel.exe';
[Environment]::SetEnvironmentVariable("BAZEL_PATH", "C:\bazel\bazel.exe", [System.EnvironmentVariableTarget]::Machine)
$systemPath = [Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::Machine)
$systemPath += ';C:\bazel'
[Environment]::SetEnvironmentVariable("PATH", $systemPath, [System.EnvironmentVariableTarget]::Machine)
