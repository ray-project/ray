#python
$Env:PYTHON_VERSION = "3.8.10"
$Env:PYTHONIOENCODING = "UTF-8"
$Env:PYTHON_RELEASE = "3.8.10"
$url = ('https://www.python.org/ftp/python/{0}/python-{1}-amd64.exe' -f $env:PYTHON_RELEASE, $env:PYTHON_VERSION); 
Write-Host ('Downloading {0} ...' -f $url); 
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
Invoke-WebRequest -Uri $url -OutFile 'pythoninstall.exe'; Write-Host 'Installing ...';
$exitCode = (Start-Process pythoninstall.exe -Wait -NoNewWindow -PassThru -ArgumentList @( '/quiet',  'InstallAllUsers=1', 'TargetDir=C:\Python38', 'PrependPath=1', 'Shortcuts=0', 'Include_doc=0', 'Include_pip=0', 'Include_test=0' )).ExitCode; 
if ($exitCode -ne 0) { 
    Write-Host ('Running python installer failed with exit code: {0}' -f $exitCode); 
    Get-ChildItem $env:TEMP | Sort-Object -Descending -Property LastWriteTime | Select-Object -First 1 | Get-Content; exit $exitCode; 
}
$env:PATH = [Environment]::GetEnvironmentVariable('PATH', [EnvironmentVariableTarget]::Machine); 
Write-Host 'Verifying install ...';
Write-Host '  python --version'; python --version;
Write-Host 'Removing ...';
Remove-Item pythoninstall.exe -Force;
Remove-Item $env:TEMP/Python*.log -Force;
Write-Host 'Complete.'


# if this is called "PIP_VERSION", pip explodes with "ValueError: invalid truth value '<VERSION>'"
$Env:PYTHON_PIP_VERSION="21.1.3"
# https://github.com/pypa/get-pip
$Env:PYTHON_GET_PIP_URL="https://github.com/pypa/get-pip/raw/a1675ab6c2bd898ed82b1f58c486097f763c74a9/public/get-pip.py"
$Env:PYTHON_GET_PIP_SHA256="6665659241292b2147b58922b9ffe11dda66b39d52d8a6f3aa310bc1d60ea6f7"

Write-Host ('Downloading get-pip.py ({0}) ...' -f $env:PYTHON_GET_PIP_URL);
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12;
Invoke-WebRequest -Uri $env:PYTHON_GET_PIP_URL -OutFile 'get-pip.py';
Write-Host ('Verifying sha256 ({0}) ...' -f $env:PYTHON_GET_PIP_SHA256);
if ((Get-FileHash 'get-pip.py' -Algorithm sha256).Hash -ne $env:PYTHON_GET_PIP_SHA256) {
    Write-Host 'FAILED!';
    exit 1;
};
Write-Host ('Installing pip=={0} ...' -f $env:PYTHON_PIP_VERSION);
python get-pip.py --disable-pip-version-check --no-cache-dir ('pip=={0}' -f $env:PYTHON_PIP_VERSION);
Remove-Item get-pip.py -Force;
Write-Host 'Verifying pip install ...';
pip --version;
Write-Host 'Complete.'
