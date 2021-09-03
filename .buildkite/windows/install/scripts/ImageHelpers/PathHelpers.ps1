function Test-MachinePath{
    [CmdletBinding()]
    param(
        [string]$PathItem
    )

    $currentPath = Get-MachinePath

    $pathItems = $currentPath.Split(';')

    if($pathItems.Contains($PathItem))
    {
        return $true
    }
    else
    {
        return $false
    }
}

function Set-MachinePath{
    [CmdletBinding()]
    param(
        [string]$NewPath
    )
    Set-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name Path -Value $NewPath
    return $NewPath
}

function Add-MachinePathItem
{
    [CmdletBinding()]
    param(
        [string]$PathItem
    )

    $currentPath = Get-MachinePath
    $newPath = $PathItem + ';' + $currentPath
    return Set-MachinePath -NewPath $newPath
}

function Get-MachinePath{
    [CmdletBinding()]
    param(

    )
    $currentPath = (Get-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name PATH).Path
    return $currentPath
}

function Get-SystemVariable{
    [CmdletBinding()]
    param(
        [string]$SystemVariable
    )
    $currentPath = (Get-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name $SystemVariable).$SystemVariable
    return $currentPath
}

function Set-SystemVariable{
    [CmdletBinding()]
    param(
        [string]$SystemVariable,
        [string]$Value
    )
    Set-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name $SystemVariable -Value $Value
    return $Value
}
