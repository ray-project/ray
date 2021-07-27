# Create Rust junction points to cargo and rustup folder
$cargoTarget = "$env:USERPROFILE\.cargo"
if (-not (Test-Path $cargoTarget))
{
    New-Item -ItemType Junction -Path $cargoTarget -Target "C:\Rust\.cargo"
}

$rustupTarget = "$env:USERPROFILE\.rustup"
if (-not (Test-Path $rustupTarget))
{
    New-Item -ItemType Junction -Path $rustupTarget -Target "C:\Rust\.rustup"
}