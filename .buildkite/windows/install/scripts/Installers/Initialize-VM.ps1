################################################################################
##  File:  Initialize-VM.ps1
##  Desc:  VM initialization script, machine level configuration
################################################################################

function Disable-InternetExplorerESC {
    $AdminKey = "HKLM:\SOFTWARE\Microsoft\Active Setup\Installed Components\{A509B1A7-37EF-4b3f-8CFC-4F3A74704073}"
    $UserKey = "HKLM:\SOFTWARE\Microsoft\Active Setup\Installed Components\{A509B1A8-37EF-4b3f-8CFC-4F3A74704073}"
    Set-ItemProperty -Path $AdminKey -Name "IsInstalled" -Value 0 -Force
    Set-ItemProperty -Path $UserKey -Name "IsInstalled" -Value 0 -Force

    $ieProcess = Get-Process -Name Explorer -ErrorAction SilentlyContinue

    if ($ieProcess){
        Stop-Process -Name Explorer -Force -ErrorAction Continue
    }

    Write-Host "IE Enhanced Security Configuration (ESC) has been disabled."
}

function Disable-InternetExplorerWelcomeScreen {
    $AdminKey = "HKLM:\Software\Policies\Microsoft\Internet Explorer\Main"
    New-Item -Path $AdminKey -Value 1 -Force
    Set-ItemProperty -Path $AdminKey -Name "DisableFirstRunCustomize" -Value 1 -Force
    Write-Host "Disabled IE Welcome screen"
}

function Disable-UserAccessControl {
    Set-ItemProperty "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System" -Name "ConsentPromptBehaviorAdmin" -Value 00000000 -Force
    Write-Host "User Access Control (UAC) has been disabled."
}

# Enable $ErrorActionPreference='Stop' for AllUsersAllHosts
Add-Content -Path $profile.AllUsersAllHosts -Value '$ErrorActionPreference="Stop"'

# Set TLS1.2
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor "Tls12"

Write-Host "Disable Antivirus nok"
#Set-MpPreference -DisableRealtimeMonitoring $true

# Disable Windows Update
#$AutoUpdatePath = "HKLM:SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU"
#If (Test-Path -Path $AutoUpdatePath) {
#    Set-ItemProperty -Path $AutoUpdatePath -Name NoAutoUpdate -Value 1
#    Write-Host "Disabled Windows Update"
#}
#else {
#    Write-Host "Windows Update key does not exist"
#}

# Install .NET Framework 3.5 (required by Chocolatey)
# Explicitly install all 4.7 sub features to include ASP.Net.
# As of  1/16/2019, WinServer 19 lists .Net 4.7 as NET-Framework-45-Features
#DISM /Online /Enable-Feature /FeatureName:NetFx3ServerFeatures 
#Install-WindowsFeature -Name NET-Framework-Features -Source C:\install\sxs\
#Install-WindowsFeature -Name NET-Framework-Features -IncludeAllSubFeature
Install-WindowsFeature -Name NET-Framework-45-Features -IncludeAllSubFeature
if (Test-IsWin16) {
    Install-WindowsFeature -Name BITS -IncludeAllSubFeature
    Install-WindowsFeature -Name DSC-Service
}

Write-Host "Disable UAC"
Disable-UserAccessControl

Write-Host "Disable IE Welcome Screen"
Disable-InternetExplorerWelcomeScreen

Write-Host "Disable IE ESC"
#Disable-InternetExplorerESC

Write-Host "Setting local execution policy"
Set-ExecutionPolicy -ExecutionPolicy Unrestricted -Scope LocalMachine  -ErrorAction Continue | Out-Null
Get-ExecutionPolicy -List

Write-Host "Enable long path behavior"
# See https://docs.microsoft.com/en-us/windows/desktop/fileio/naming-a-file#maximum-path-length-limitation
Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' -Name 'LongPathsEnabled' -Value 1

Write-Host "Install chocolatey"
$chocoExePath = 'C:\ProgramData\Chocolatey\bin'

if ($($env:Path).ToLower().Contains($($chocoExePath).ToLower())) {
    Write-Host "Chocolatey found in PATH, skipping install..."
    Exit
}

# Add to system PATH
$systemPath = [Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::Machine)
$systemPath += ';' + $chocoExePath
[Environment]::SetEnvironmentVariable("PATH", $systemPath, [System.EnvironmentVariableTarget]::Machine)

# Update local process' path
$userPath = [Environment]::GetEnvironmentVariable('Path', [System.EnvironmentVariableTarget]::User)
if ($userPath) {
    $env:Path = $systemPath + ";" + $userPath
}
else {
    $env:Path = $systemPath
}

# Run the installer
Invoke-Expression ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1'))

# Turn off confirmation
choco feature enable -n allowGlobalConfirmation

# https://github.com/chocolatey/choco/issues/89
# Remove some of the command aliases, like `cpack` #89
Remove-Item -Path $env:ChocolateyInstall\bin\cpack.exe -Force

if (Test-IsWin16) {
    # Install vcredist140
    Choco-Install -PackageName vcredist140
}

if (Test-IsWin19) {
    # Install vcredist2010
    $Vc2010x86Name = "vcredist_x86.exe"
    $Vc2010x86URI = "https://download.microsoft.com/download/1/6/5/165255E7-1014-4D0A-B094-B6A430A6BFFC/${Vc2010x86Name}"
    $Vc2010x64Name = "vcredist_x64.exe"
    $Vc2010x64URI = "https://download.microsoft.com/download/1/6/5/165255E7-1014-4D0A-B094-B6A430A6BFFC/${Vc2010x64Name}"
    $ArgumentList = ("/install", "/quiet", "/norestart")
    Install-Binary -Url $Vc2010x86URI -Name $Vc2010x86Name -ArgumentList $ArgumentList
    Install-Binary -Url $Vc2010x64URI -Name $Vc2010x64Name -ArgumentList $ArgumentList
}

# Initialize environmental variable ChocolateyToolsLocation by invoking choco Get-ToolsLocation function
Import-Module "$env:ChocolateyInstall\helpers\chocolateyInstaller.psm1" -Force
Get-ToolsLocation
