#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs the Ray lint/format script in a bash environment with conda activation on Windows.

.DESCRIPTION
    This script detects the current conda environment and runs the Ray format script
    (ci/lint/format.sh) in a bash session with the conda environment properly activated.
    
    Assumptions:
    - Conda is the Python environment manager
    - Git Bash is installed to either the default location or is available from $Env:PATH
    
    The script will:
    1. Check if conda is available by looking for $Env:_CONDA_EXE environment variable
    2. Detect the current conda environment name from $Env:CONDA_DEFAULT_ENV
    3. Start a bash session that activates the conda environment
    4. Run the Ray format script within that environment

.EXAMPLE
    .\scripts\format.ps1
#>

# Set error action preference to stop on errors
$ErrorActionPreference = "Stop"

Write-Host "Ray Format Script - PowerShell Wrapper" -ForegroundColor Green
Write-Host "=======================================" -ForegroundColor Green

# Check if conda is available
Write-Host "Checking for conda installation..." -ForegroundColor Yellow

if (-not $env:_CONDA_EXE) {
    Write-Error @"
ERROR: Conda is not available or not properly initialized.

Please run this powershell script from a terminal from which 
the desired conda environment has been activated.

Current environment variables:
_CONDA_EXE: $env:_CONDA_EXE
CONDA_DEFAULT_ENV: $env:CONDA_DEFAULT_ENV
"@
    exit 1
}

Write-Host "[OK] Conda found at: $env:_CONDA_EXE" -ForegroundColor Green

# Check for current conda environment
if (-not $env:CONDA_DEFAULT_ENV) {
    Write-Warning "No conda environment is currently active (CONDA_DEFAULT_ENV is not set)."
    Write-Host "Available conda environments:" -ForegroundColor Yellow
    & conda env list
    Write-Error @"
ERROR: No conda environment is active.

Please activate a conda environment before running this script:
    conda activate your-environment-name
"@
    exit 1
}

$condaEnv = $env:CONDA_DEFAULT_ENV
Write-Host "[OK] Current conda environment: $condaEnv" -ForegroundColor Green

# Find bash executable
$bashPath = $null
$possibleBashPaths = @(
    "C:\Program Files\Git\bin\bash.exe",
    "C:\Program Files (x86)\Git\bin\bash.exe",
    "bash.exe"
)

foreach ($path in $possibleBashPaths) {
    $foundCommand = Get-Command $path -ErrorAction SilentlyContinue
    if ($foundCommand) {
        $bashPath = $foundCommand.Source
        break
    }
}

if (-not $bashPath) {
    Write-Error @"
ERROR: Bash executable not found.
"@
    exit 1
}

Write-Host "[OK] Bash found at: $bashPath" -ForegroundColor Green

# Find Git repository root and verify format script exists
try {
    $gitRoot = git rev-parse --show-toplevel 2>$null
    if (-not $gitRoot) {
        throw "Not in a Git repository"
    }
    
    # Convert Unix-style path to Windows path if needed
    $repoRoot = $gitRoot -replace '/', '\'
    $formatScriptPath = Join-Path $repoRoot "ci\lint\format.sh"
    
    if (-not (Test-Path $formatScriptPath)) {
        Write-Error @"
ERROR: Ray format script not found at expected location.

Repository root: $repoRoot
Expected script path: $formatScriptPath

This script must be run from within the Ray repository.
"@
        exit 1
    }
    
    Write-Host "[OK] Foundformat script at: $formatScriptPath" -ForegroundColor Green
    
} catch {
    Write-Error @"
ERROR: Could not locate Ray repository or format script.

This script must be run from within the Ray Git repository.
Current directory: $(Get-Location)

Error details: $($_.Exception.Message)
"@
    exit 1
}

# Prepare the bash command
# Create the bash command that will:
# 1. Source conda initialization
# 2. Activate the conda environment  
# 3. Change to repository root
# 4. Run the format script (default behavior - changed files only)
$bashCommand = @"
# Initialize conda for bash
eval "`$(conda shell.bash hook)"

# Activate the conda environment
echo "Activating conda environment: $condaEnv"
conda activate $condaEnv

# Verify environment is active
echo "Verifying active conda environment: `$CONDA_DEFAULT_ENV"
echo "Python location: `$(which python)"

# Change to repository root and run the format script
cd "$($gitRoot)"
echo "Running format script on changed files..."
./ci/lint/format.sh
"@

Write-Host "`nStarting bash session with conda environment activation..." -ForegroundColor Yellow

# Execute the bash command
try {
    & $bashPath -c $bashCommand
    $exitCode = $LASTEXITCODE
    
    if ($exitCode -eq 0) {
        Write-Host "`n[SUCCESS] Format script completed successfully!" -ForegroundColor Green
    } else {
        Write-Host "`n[ERROR] Format script failed with exit code: $exitCode" -ForegroundColor Red
        exit $exitCode
    }
} catch {
    Write-Error "Failed to execute bash command: $_"
    exit 1
}
