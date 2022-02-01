Write-Host "Disable Just-In-Time Debugger"

# Turn off Application Error Debugger
New-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\AeDebug" -Name Debugger -Value "-" -Type String -Force
New-ItemProperty -Path "HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows NT\CurrentVersion\AeDebug" -Name Debugger -Value "-" -Type String -Force

# Turn off the Debug dialog
New-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\.NETFramework" -Name DbgManagedDebugger -Value "-" -Type String -Force
New-ItemProperty -Path "HKLM:\SOFTWARE\WOW6432Node\Microsoft\.NETFramework" -Name DbgManagedDebugger -Value "-" -Type String -Force

# Disable the WER UI
New-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\Windows Error Reporting" -Name DontShowUI -Value 1 -Type DWORD -Force
# Send all reports to the user's queue
New-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\Windows Error Reporting" -Name ForceQueue -Value 1 -Type DWORD -Force
# Default consent choice 1 - Always ask (default)
New-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\Windows Error Reporting\Consent" -Name DefaultConsent -Value 1 -Type DWORD -Force