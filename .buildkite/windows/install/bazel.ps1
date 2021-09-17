$Env:BAZEL_URL="https://github.com/bazelbuild/bazel/releases/download/3.2.0/bazel-3.2.0-windows-x86_64.zip"
Write-Host ('Downloading {0} ...' -f $env:BAZEL_URL);
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
Invoke-WebRequest -Uri $env:BAZEL_URL -OutFile 'bazel.zip';
Write-Host 'Expanding ...';
New-Item -ItemType Directory -Path C:\bazel -Force | Out-Null;
Expand-Archive bazel.zip -DestinationPath C:\bazel -Force;
Write-Host 'Removing ...';
Remove-Item bazel.zip -Force;
[Environment]::SetEnvironmentVariable("BAZEL_PATH", "C:\bazel\bazel.exe", [System.EnvironmentVariableTarget]::Machine)
$systemPath = [Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::Machine)
$systemPath += ';C:\bazel'
[Environment]::SetEnvironmentVariable("PATH", $systemPath, [System.EnvironmentVariableTarget]::Machine)
