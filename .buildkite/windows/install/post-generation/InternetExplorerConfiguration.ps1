# https://docs.microsoft.com/en-us/troubleshoot/browsers/enhanced-security-configuration-faq#how-to-disable-internet-explorer-esc-by-using-a-script
# turn off the Internet Explorer Enhanced Security feature
Rundll32 iesetup.dll, IEHardenLMSettings, 1, True
Rundll32 iesetup.dll, IEHardenUser, 1, True
Rundll32 iesetup.dll, IEHardenAdmin, 1, True
$AdminKey = "HKLM:\SOFTWARE\Microsoft\Active Setup\Installed Components\{A509B1A7-37EF-4b3f-8CFC-4F3A74704073}"
$UserKey = "HKLM:\SOFTWARE\Microsoft\Active Setup\Installed Components\{A509B1A8-37EF-4b3f-8CFC-4F3A74704073}"
Set-ItemProperty -Path $AdminKey -Name "IsInstalled" -Value 0 -Force
Set-ItemProperty -Path $UserKey -Name "IsInstalled" -Value 0 -Force
# restart Explorer process
$ieProcess = Get-Process -Name Explorer -ErrorAction Ignore
if ($ieProcess) {
    Stop-Process -Name Explorer -Force -ErrorAction Ignore
}
Write-Host "IE Enhanced Security Configuration (ESC) has been disabled."
