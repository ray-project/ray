#git
$Env:GIT_VERSION = "2.32.0"
$Env:GIT_SUBVERSION = "2"
$url = ('https://github.com/git-for-windows/git/releases/download/v{0}.windows.{1}/Git-{0}.{1}-64-bit.exe' -f $env:GIT_VERSION, $env:GIT_SUBVERSION); 
Write-Host ('Downloading {0} ...' -f $url); 
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
Invoke-WebRequest -Uri $url -OutFile 'gitinstall.exe'; Write-Host 'Installing ...';
$exitCode = (Start-Process gitinstall.exe -Wait -NoNewWindow -PassThru -ArgumentList @( '/VERYSILENT', '/NORESTART', '/GitAndUnixToolsOnPath' )).ExitCode; 
if ($exitCode -ne 0) { 
    Write-Host ('Running git installer failed with exit code: {0}' -f $exitCode); 
    Get-ChildItem $env:TEMP | Sort-Object -Descending -Property LastWriteTime | Select-Object -First 1 | Get-Content; exit $exitCode; 
}
Write-Host 'Complete.'
