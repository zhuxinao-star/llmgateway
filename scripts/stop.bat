@echo off
setlocal
rem -----------------------------------------------------------------------
rem Stop llmGateway by the process listening on the gateway port.
rem Never by image name - other java processes on this machine stay alive.
rem -----------------------------------------------------------------------
call "%~dp0_env.bat"
if errorlevel 1 exit /b 1

echo [stop] port %GW_PORT%
powershell -NoProfile -Command "$c = Get-NetTCPConnection -LocalPort %GW_PORT% -State Listen -ErrorAction SilentlyContinue; if ($c) { $p = ($c | Select-Object -First 1).OwningProcess; Stop-Process -Id $p -Force; Write-Output ('  [stopped] PID=' + $p) } else { Write-Output '  [not running] no listener on this port' }"
endlocal
