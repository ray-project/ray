$Env:JAVA_HOME="C:\openjdk-16"
$newPath = ('{0}\bin;{1}' -f $env:JAVA_HOME, $env:PATH);
Write-Host ('Updating PATH: {0}' -f $newPath);
setx /M PATH $newPath;
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
Write-Host 'Complete.'

$Env:JAVA_VERSION="16.0.1"
$Env:JAVA_URL="https://download.java.net/java/GA/jdk16.0.1/7147401fd7354114ac51ef3e1328291f/9/GPL/openjdk-16.0.1_windows-x64_bin.zip"
$Env:JAVA_SHA256="733b45b09463c97133d70c2368f1b9505da58e88f2c8a84358dd4accfd06a7a4"

Write-Host ('Downloading {0} ...' -f $env:JAVA_URL);
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
Invoke-WebRequest -Uri $env:JAVA_URL -OutFile 'openjdk.zip';
Write-Host ('Verifying sha256 ({0}) ...' -f $env:JAVA_SHA256);
if ((Get-FileHash openjdk.zip -Algorithm sha256).Hash -ne $env:JAVA_SHA256) {
    Write-Host 'FAILED!';
    exit 1;
};

Write-Host 'Expanding ...';
New-Item -ItemType Directory -Path C:\temp -Force | Out-Null;
Expand-Archive openjdk.zip -DestinationPath C:\temp -Force;
Move-Item -Path C:\temp\* -Destination $env:JAVA_HOME;
Remove-Item C:\temp;

Write-Host 'Removing ...';
Remove-Item openjdk.zip -Force;

$Env:JAVA_HOME="/c/openjdk-16"
setx /M JAVA_HOME $Env:JAVA_HOME

Write-Host 'Verifying install ...';
Write-Host '  javac --version'; javac --version;
Write-Host '  java --version'; java --version;
Write-Host 'Complete.'