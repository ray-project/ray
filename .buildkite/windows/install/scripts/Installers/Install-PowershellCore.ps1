################################################################################
##  File:  Install-PowershellCore.ps1
##  Desc:  Install PowerShell Core
################################################################################

Invoke-Expression "& { $(Invoke-RestMethod https://aka.ms/install-powershell.ps1) } -UseMSI -Quiet"

# about_update_notifications
# While the update check happens during the first session in a given 24-hour period, for performance reasons,
# the notification will only be shown on the start of subsequent sessions.
# Also for performance reasons, the check will not start until at least 3 seconds after the session begins.
[System.Environment]::SetEnvironmentVariable("POWERSHELL_UPDATECHECK", "Off", [System.EnvironmentVariableTarget]::Machine)

