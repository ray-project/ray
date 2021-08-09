$env:MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-py38_4.10.3-Windows-x86_64.exe"
Write-Host ('Downloading {0} ...' -f $env:MINICONDA_URL);
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
Invoke-WebRequest -Uri $env:MINICONDA_URL -OutFile 'Miniconda3-latest.exe';
$exitCode = (Start-Process Miniconda3-latest.exe -Wait -NoNewWindow -PassThru -ArgumentList @('/S', '/InstallationType=AllUsers', '/D=C:\Miniconda3' )).ExitCode; 
if ($exitCode -ne 0) { 
    Write-Host ('Running Miniconda3 installer failed with exit code: {0}' -f $exitCode); 
    Get-ChildItem $env:TEMP | Sort-Object -Descending -Property LastWriteTime | Select-Object -First 1 | Get-Content; exit $exitCode; 
}
Write-Host 'Removing ...';
Remove-Item Miniconda3-latest.exe -Force;
# Add to system PATH
$systemPath = [Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::Machine)
$systemPath += ';C:\Miniconda3;C:\Miniconda3\Scripts;C:\Miniconda3\Library\bin'
[Environment]::SetEnvironmentVariable("PATH", $systemPath, [System.EnvironmentVariableTarget]::Machine)
