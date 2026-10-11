@echo off
setlocal
rem -----------------------------------------------------------------------
rem Is the gateway up? There is no /api/health endpoint - /api/overview is
rem the liveness probe. "status.bat log" also tails the log file.
rem -----------------------------------------------------------------------
call "%~dp0_env.bat"
if errorlevel 1 exit /b 1

echo probe http://%GW_HOST%:%GW_PORT%/api/overview
powershell -NoProfile -Command "try { $r = Invoke-WebRequest -Uri 'http://%GW_HOST%:%GW_PORT%/api/overview' -UseBasicParsing -TimeoutSec 5; Write-Output ('  UP HTTP=' + $r.StatusCode) } catch { Write-Output ('  DOWN: ' + $_.Exception.Message) }"

if /i not "%~1"=="log" goto :no_log
echo.
echo last 20 lines of %GW_LOG_DIR%\app.log
powershell -NoProfile -Command "if (Test-Path -LiteralPath '%GW_LOG_DIR%\app.log') { Get-Content -LiteralPath '%GW_LOG_DIR%\app.log' -Tail 20 } else { Write-Output '  (no app.log yet)' }"
exit /b 0

:no_log
echo.
echo show recent log lines: scripts\status.bat log
exit /b 0
