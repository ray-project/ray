echo Mainprocess
$Env:TOOLSET_JSON_PATH = "C:/Install/toolset/toolset-2019.json"
echo $Env:TOOLSET_JSON_PATH
Set-Service -Name wuauserv -StartupType Manual; Start-Service -Name wuauserv;
Import-Module -Name C:/Install/scripts/ImageHelpers -Verbose
Get-Command -Module C:/Install/scripts/ImageHelpers
./scripts/Installers/Install-PowerShellModules.ps1
./scripts/Installers/Initialize-VM.ps1
./scripts/Installers/Update-DotnetTLS.ps1
./scripts/Installers/Install-PowershellCore.ps1
./scripts/Installers/Install-VS.ps1
./scripts/Installers/Disable-JITDebugger.ps1
./scripts/Installers/Enable-DeveloperMode.ps1
./scripts/Installers/Finalize-VM.ps1
choco install vim
