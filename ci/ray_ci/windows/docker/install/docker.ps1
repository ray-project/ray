msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi /qn
$dockerUrl = "https://download.docker.com/win/static/stable/x86_64/docker-20.10.10.zip"
Write-Host ('Downloading {0} ...' -f $dockerUrl);
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
Invoke-WebRequest -Uri $dockerUrl -OutFile 'C:\docker-20.10.10.zip';
Expand-Archive -Path 'C:\docker-20.10.10.zip' -DestinationPath 'C:\' -Force
$systemPath = [Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::Machine)
$systemPath += ';C:\docker'
[Environment]::SetEnvironmentVariable("PATH", $systemPath, [System.EnvironmentVariableTarget]::Machine)
