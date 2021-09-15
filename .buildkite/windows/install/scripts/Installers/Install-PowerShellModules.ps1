dir C:\Install\scripts\


# Set TLS1.2
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor "Tls12"

Write-Host "Setup PowerShellGet"
Install-PackageProvider -Name NuGet -MinimumVersion 2.8.5.201 -Force

# Specifies the installation policy
Set-PSRepository -InstallationPolicy Trusted -Name PSGallery

# Install PowerShell modules
$modules = (Get-ToolsetContent).powershellModules

foreach($module in $modules)
{
    $moduleName = $module.name
    Write-Host "Installing ${moduleName} module"

    if ($module.versions)
    {
        foreach ($version in $module.versions)
        {
            Write-Host " - $version"
            Install-Module -Name $moduleName -RequiredVersion $version -Scope AllUsers -SkipPublisherCheck -Force
        }
        continue
    }

    Install-Module -Name $moduleName -Scope AllUsers -SkipPublisherCheck -Force
}
