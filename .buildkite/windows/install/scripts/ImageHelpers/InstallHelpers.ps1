
$Env:TOOLSET_JSON_PATH = "C:/Install/toolset/toolset-2019.json"
echo $Env:TOOLSET_JSON_PATH

function Install-Binary
{
    <#
    .SYNOPSIS
        A helper function to install executables.

    .DESCRIPTION
        Download and install .exe or .msi binaries from specified URL.

    .PARAMETER Url
        The URL from which the binary will be downloaded. Required parameter.

    .PARAMETER Name
        The Name with which binary will be downloaded. Required parameter.

    .PARAMETER ArgumentList
        The list of arguments that will be passed to the installer. Required for .exe binaries.

    .EXAMPLE
        Install-Binary -Url "https://go.microsoft.com/fwlink/p/?linkid=2083338" -Name "winsdksetup.exe" -ArgumentList ("/features", "+", "/quiet")
    #>

    Param
    (
        [Parameter(Mandatory)]
        [String] $Url,
        [Parameter(Mandatory)]
        [String] $Name,
        [String[]] $ArgumentList
    )

    Write-Host "Downloading $Name..."
    $filePath = Start-DownloadWithRetry -Url $Url -Name $Name

    # MSI binaries should be installed via msiexec.exe
    $fileExtension = ([System.IO.Path]::GetExtension($Name)).Replace(".", "")
    if ($fileExtension -eq "msi")
    {
        $ArgumentList = ('/i', $filePath, '/QN', '/norestart')
        $filePath = "msiexec.exe"
    }

    try
    {
        Write-Host "Starting Install $Name..."
        $process = Start-Process -FilePath $filePath -ArgumentList $ArgumentList -Wait -PassThru

        $exitCode = $process.ExitCode
        if ($exitCode -eq 0 -or $exitCode -eq 3010)
        {
            Write-Host "Installation successful"
        }
        else
        {
            Write-Host "Non zero exit code returned by the installation process: $exitCode"
            exit $exitCode
        }
    }
    catch
    {
        Write-Host "Failed to install the $fileExtension ${Name}: $($_.Exception.Message)"
        exit 1
    }
}

function Stop-SvcWithErrHandling
{
    <#
    .DESCRIPTION
        Function for stopping the Windows Service with error handling

    .PARAMETER ServiceName
        The name of stopping service

    .PARAMETER StopOnError
        Switch for stopping the script and exit from PowerShell if one service is absent
    #>
    Param
    (
        [Parameter(Mandatory, ValueFromPipeLine = $true)]
        [string] $ServiceName,
        [switch] $StopOnError
    )

    Process
    {
        $service = Get-Service $ServiceName -ErrorAction SilentlyContinue
        if (-not $service)
        {
            Write-Warning "[!] Service [$ServiceName] is not found"
            if ($StopOnError)
            {
                exit 1
            }

        }
        else
        {
            Write-Host "Try to stop service [$ServiceName]"
            try
            {
                Stop-Service -Name $ServiceName -Force
                $service.WaitForStatus("Stopped", "00:01:00")
                Write-Host "Service [$ServiceName] has been stopped successfuly"
            }
            catch
            {
                Write-Error "[!] Failed to stop service [$ServiceName] with error:"
                $_ | Out-String | Write-Error
            }
        }
    }
}

function Set-SvcWithErrHandling
{
    <#
    .DESCRIPTION
        Function for setting the Windows Service parameter with error handling

    .PARAMETER ServiceName
        The name of stopping service

    .PARAMETER Arguments
        Hashtable for service arguments
    #>

    Param
    (
        [Parameter(Mandatory, ValueFromPipeLine = $true)]
        [string] $ServiceName,
        [Parameter(Mandatory)]
        [hashtable] $Arguments
    )

    Process
    {
        $service = Get-Service $ServiceName -ErrorAction SilentlyContinue
        if (-not $service)
            {
                Write-Warning "[!] Service [$ServiceName] is not found"
            }

        try
        {
           Set-Service $serviceName @Arguments
        }
        catch
        {
            Write-Error "[!] Failed to set service [$ServiceName] arguments with error:"
            $_ | Out-String | Write-Error
        }
    }
}

function Start-DownloadWithRetry
{
    Param
    (
        [Parameter(Mandatory)]
        [string] $Url,
        [string] $Name,
        [string] $DownloadPath = "${env:Temp}",
        [int] $Retries = 20
    )

    if ([String]::IsNullOrEmpty($Name)) {
        $Name = [IO.Path]::GetFileName($Url)
    }

    $filePath = Join-Path -Path $DownloadPath -ChildPath $Name

    #Default retry logic for the package.
    while ($Retries -gt 0)
    {
        try
        {
            Write-Host "Downloading package from: $Url to path $filePath ."
            (New-Object System.Net.WebClient).DownloadFile($Url, $filePath)
            break
        }
        catch
        {
            Write-Host "There is an error during package downloading:`n $_"
            $Retries--

            if ($Retries -eq 0)
            {
                Write-Host "File can't be downloaded. Please try later or check that file exists by url: $Url"
                exit 1
            }

            Write-Host "Waiting 30 seconds before retrying. Retries left: $Retries"
            Start-Sleep -Seconds 30
        }
    }

    return $filePath
}

function Get-VsixExtenstionFromMarketplace {
    Param
    (
        [string] $ExtensionMarketPlaceName,
        [string] $MarketplaceUri = "https://marketplace.visualstudio.com/items?itemName="
    )

    $extensionUri = $MarketplaceUri + $ExtensionMarketPlaceName
    $request = Invoke-WebRequest -Uri $extensionUri -UseBasicParsing
    $request -match 'UniqueIdentifierValue":"(?<extensionname>[^"]*)' | Out-Null
    $extensionName = $Matches.extensionname
    $request -match 'VsixId":"(?<vsixid>[^"]*)' | Out-Null
    $vsixId = $Matches.vsixid
    $request -match 'AssetUri":"(?<uri>[^"]*)' | Out-Null
    $assetUri = $Matches.uri
    $request -match 'Microsoft\.VisualStudio\.Services\.Payload\.FileName":"(?<filename>[^"]*)' | Out-Null
    $fileName = $Matches.filename
    $downloadUri = $assetUri + "/" + $fileName

    return [PSCustomObject] @{
        "ExtensionName" = $extensionName
        "VsixId" = $vsixId
        "FileName" = $fileName
        "DownloadUri" = $downloadUri
    }
}

function Install-VsixExtension
{
    Param
    (
        [string] $Url,
        [Parameter(Mandatory = $true)]
        [string] $Name,
        [string] $FilePath,
        [Parameter(Mandatory = $true)]
        [string] $VSversion,
        [int] $retries = 20,
        [switch] $InstallOnly
    )

    if (-not $InstallOnly)
        {
            $FilePath = Start-DownloadWithRetry -Url $Url -Name $Name
        }

    $argumentList = ('/quiet', "`"$FilePath`"")

    Write-Host "Starting Install $Name..."
    $vsEdition = (Get-ToolsetContent).visualStudio.edition
    try
    {
        #There are 2 types of packages at the moment - exe and vsix
        if ($Name -match "vsix")
        {
            $process = Start-Process -FilePath "C:\Program Files (x86)\Microsoft Visual Studio\$VSversion\${vsEdition}\Common7\IDE\VSIXInstaller.exe" -ArgumentList $argumentList -Wait -PassThru
        }
        else
        {
            $process = Start-Process -FilePath ${env:Temp}\$Name /Q -Wait -PassThru
        }
    }
    catch
    {
        Write-Host "There is an error during $Name installation"
        $_
        exit 1
    }

    $exitCode = $process.ExitCode

    if ($exitCode -eq 0 -or $exitCode -eq 1001) # 1001 means the extension is already installed
    {
        Write-Host "$Name installed successfully"
    }
    else
    {
        Write-Host "Unsuccessful exit code returned by the installation process: $exitCode."
        exit 1
    }

    #Cleanup downloaded installation files
    if (-not $InstallOnly)
        {
            Remove-Item -Force -Confirm:$false $FilePath
        }
}

function Get-VSExtensionVersion
{
    Param
    (
        [Parameter(Mandatory=$true)]
        [string] $packageName
    )

    $instanceFolders = Get-ChildItem -Path "C:\ProgramData\Microsoft\VisualStudio\Packages\_Instances"
    if ($instanceFolders -is [array])
    {
        Write-Host ($instanceFolders | Out-String)
        Write-Host ($instanceFolders | Get-ChildItem | Out-String)
        Write-Host "More than one instance installed"
        exit 1
    }

    $stateContent = Get-Content -Path (Join-Path $instanceFolders.FullName '\state.packages.json')
    $state = $stateContent | ConvertFrom-Json
    $packageVersion = ($state.packages | Where-Object { $_.id -eq $packageName }).version

    if (-not $packageVersion)
    {
        Write-Host "Installed package $packageName for Visual Studio was not found"
        exit 1
    }

    return $packageVersion
}

function Get-ToolsetContent
{
    $toolsetJson = Get-Content -Path $env:TOOLSET_JSON_PATH -Raw
    ConvertFrom-Json -InputObject $toolsetJson
}

function Get-ToolcacheToolDirectory {
    Param ([string] $ToolName)
    $toolcacheRootPath = Resolve-Path $env:AGENT_TOOLSDIRECTORY
    return Join-Path $toolcacheRootPath $ToolName
}

function Get-ToolsetToolFullPath
{
    <#
    .DESCRIPTION
        Function that return full path to specified toolset tool.

    .PARAMETER Name
        The name of required tool.

    .PARAMETER Version
        The version of required tool.

    .PARAMETER Arch
        The architecture of required tool.
    #>

    Param
    (
        [Parameter(Mandatory=$true)]
        [string] $Name,
        [Parameter(Mandatory=$true)]
        [string] $Version,
        [string] $Arch = "x64"
    )

    $toolPath = Get-ToolcacheToolDirectory -ToolName $Name

    # Add wildcard if missing
    if ($Version.Split(".").Length -lt 3) {
        $Version += ".*"
    }

    $versionPath = Join-Path $toolPath $Version

    # Take latest installed version in case if toolset version contains wildcards
    $foundVersion = Get-Item $versionPath `
                    | Sort-Object -Property {[version]$_.name} -Descending `
                    | Select-Object -First 1

    if (-not $foundVersion) {
        return $null
    }

    return Join-Path $foundVersion $Arch
}

function Get-WinVersion
{
    (Get-CimInstance -ClassName Win32_OperatingSystem).Caption
}

function Test-IsWin19
{
    (Get-WinVersion) -match "2019"
}

function Test-IsWin16
{
    (Get-WinVersion) -match "2016"
}
