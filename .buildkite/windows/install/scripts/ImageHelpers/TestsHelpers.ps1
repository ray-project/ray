function Get-CommandResult {
    Param (
        [Parameter(Mandatory)][string] $Command
    )
    # CMD trick to suppress and show error output because some commands write to stderr (for example, "python --version")
    [string[]]$output = & $env:comspec /c "$Command 2>&1"
    $exitCode = $LASTEXITCODE

    return @{
        Output = $output
        ExitCode = $exitCode
    }
}

# Gets path to the tool, analogue of 'which tool'
function Get-WhichTool($tool) {
    return (Get-Command $tool).Path
}

# Gets value of environment variable by the name
function Get-EnvironmentVariable($variable) {
    return [System.Environment]::GetEnvironmentVariable($variable, "Machine")
}

# Update environment variables without reboot
function Update-Environment {
    $variables = [Environment]::GetEnvironmentVariables("Machine")
    $variables.Keys | ForEach-Object {
        $key = $_
        $value = $variables[$key]
        Set-Item -Path "env:$key" -Value $value
    }
    # We need to refresh PATH the latest one because it could include other variables "%M2_HOME%/bin"
    $env:PATH = [Environment]::GetEnvironmentVariable("PATH", "Machine")
}

# Run Pester tests for specific tool
function Invoke-PesterTests {
    Param(
        [Parameter(Mandatory)][string] $TestFile,
        [string] $TestName
    )

    $testPath = "C:\Install\scripts\Tests\${TestFile}.Tests.ps1"
    if (-not (Test-Path $testPath)) {
        throw "Unable to find test file '$TestFile' on '$testPath'."
    }

    $configuration = [PesterConfiguration] @{
        Run = @{ Path = $testPath; PassThru = $true }
        Output = @{ Verbosity = "Detailed" }
    }
    if ($TestName) {
        $configuration.Filter.FullName = $TestName
    }

    # Update environment variables without reboot
    Update-Environment

    # Switch ErrorActionPreference to Stop temporary to make sure that tests will on silent errors too
    $backupErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Stop"
    $results = Invoke-Pester -Configuration $configuration
    $ErrorActionPreference = $backupErrorActionPreference

    # Fail in case if no tests are run
    if (-not ($results -and ($results.FailedCount -eq 0) -and ($results.PassedCount -gt 0))) {
        $results
        throw "Test run has failed"
    }
}

# Pester Assert to check exit code of command
function ShouldReturnZeroExitCode {
    Param(
        [String] $ActualValue,
        [switch] $Negate,
        [string] $Because
    )

    $result = Get-CommandResult $ActualValue

    [bool]$succeeded = $result.ExitCode -eq 0
    if ($Negate) { $succeeded = -not $succeeded }

    if (-not $succeeded)
    {
        $commandOutputIndent = " " * 4
        $commandOutput = ($result.Output | ForEach-Object { "${commandOutputIndent}${_}" }) -join "`n"
        $failureMessage = "Command '${ActualValue}' has finished with exit code ${actualExitCode}`n${commandOutput}"
    }

    return [PSCustomObject] @{
        Succeeded      = $succeeded
        FailureMessage = $failureMessage
    }
}

# Pester Assert to match output of command
function ShouldMatchCommandOutput {
    Param(
        [String] $ActualValue,
        [String] $RegularExpression,
        [switch] $Negate
    )

    $output = (Get-CommandResult $ActualValue).Output | Out-String
    [bool] $succeeded = $output -cmatch $RegularExpression

    if ($Negate) {
        $succeeded = -not $succeeded
    }

    $failureMessage = ''

    if (-not $succeeded) {
        if ($Negate) {
            $failureMessage = "Expected regular expression '$RegularExpression' for '$ActualValue' command to not match '$output', but it did match."
        }
        else {
            $failureMessage = "Expected regular expression '$RegularExpression' for '$ActualValue' command to match '$output', but it did not match."
        }
    }

    return [PSCustomObject] @{
        Succeeded      = $succeeded
        FailureMessage = $failureMessage
    }
}

If (Get-Command -Name Add-ShouldOperator -ErrorAction SilentlyContinue) {
    Add-ShouldOperator -Name ReturnZeroExitCode -InternalName ShouldReturnZeroExitCode -Test ${function:ShouldReturnZeroExitCode}
    Add-ShouldOperator -Name MatchCommandOutput -InternalName ShouldMatchCommandOutput -Test ${function:ShouldMatchCommandOutput}
}