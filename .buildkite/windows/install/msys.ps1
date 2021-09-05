#Expand-Archive msys64.zip -DestinationPath C:\ -Force;
$url = 'https://github.com/msys2/msys2-installer/releases/download/nightly-x86_64/msys2-base-x86_64-latest.sfx.exe'

Write-Host ('Downloading {0} ...' -f $url); 
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
Invoke-WebRequest -Uri $url -OutFile 'msysbase.exe'; Write-Host 'Installing msys2 ...';
$exitCode = (Start-Process msysbase.exe -Wait -NoNewWindow -PassThru -ArgumentList @( '-y', '-oC:\' )).ExitCode; 
if ($exitCode -ne 0) { 
    Write-Host ('Running msys2 installer failed with exit code: {0}' -f $exitCode); 
    Get-ChildItem $env:TEMP | Sort-Object -Descending -Property LastWriteTime | Select-Object -First 1 | Get-Content; exit $exitCode; 
}
Write-Host 'Complete.'

$systemPath = [Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::Machine)
$systemPath += ';C:\msys64\mingw64\bin;C:\msys64\usr\bin'
[Environment]::SetEnvironmentVariable("PATH", $systemPath, [System.EnvironmentVariableTarget]::Machine)
$env:BAZEL_SH = "C:\msys64\usr\bin\bash.exe"
[Environment]::SetEnvironmentVariable("BAZEL_SH", $env:BAZEL_SH, [System.EnvironmentVariableTarget]::Machine)
$env:HOME = $env:USERPROFILE
[Environment]::SetEnvironmentVariable("HOME", $env:HOME, [System.EnvironmentVariableTarget]::Machine)
# Update local process' path
$userPath = [Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::User)
if ($userPath) {
    $env:Path = $systemPath + ";" + $userPath
}
else {
    $env:Path = $systemPath
}

Write-Host "`n$dash bash pacman-key --init"
bash.exe -c "pacman-key --init 2>&1"

Write-Host "bash pacman-key --populate msys2"
bash.exe -c "pacman-key --populate msys2 2>&1"

Write-Host "`n$dash pacman --noconfirm -Syyuu"
pacman.exe -Syyuu --noconfirm
taskkill /f /fi "MODULES eq msys-2.0.dll"
Write-Host "`n$dash pacman --noconfirm -Syuu (2nd pass)"
pacman.exe -Syuu  --noconfirm
taskkill /f /fi "MODULES eq msys-2.0.dll"

bash -c "pacman --noconfirm -Scc"
bash -c "pacman -S --noconfirm unzip"
bash -c "pacman -S --noconfirm p7zip"
