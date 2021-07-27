$vsInstallRoot = Get-VisualStudioPath
$devEnvPath = "$vsInstallRoot\Common7\IDE\devenv.exe"

cmd.exe /c "`"$devEnvPath`" /updateconfiguration"