@echo off
setlocal
rem -----------------------------------------------------------------------
rem Stop llmGateway by the process listening on the gateway port.
rem
rem Two rules the old version broke, which is why scripts\stop.bat "did nothing":
rem  1. no PowerShell. This machine refuses to start powershell.exe from a batch
rem     file (access denied, rc=5), so the old Stop-Process line never ran and the
rem     script printed nothing at all.
rem  2. never kill by process name, and now also never kill by port alone. The
rem     port is shared with another java program on this machine, so the listener's
rem     command line has to say llmGateway before it is killed. _gw.bat does that
rem     checking for every script here.
rem -----------------------------------------------------------------------
call "%~dp0_env.bat"
if errorlevel 1 exit /b 1

call "%~dp0_gw.bat" %GW_PORT%
if not defined GW_PID goto :not_running

if "%GW_OWN%"=="1" goto :kill
if "%GW_OWN%"=="-1" goto :unknown

echo [REFUSED] port %GW_PORT% is held by another program, not llmGateway.
echo           it is: %GW_PROC%
echo           Nothing was killed. Stop that program with its own script, or move
echo           one of the two off the port (see scripts\doctor.bat, GW_PORT).
exit /b 1

:unknown
echo [REFUSED] something listens on port %GW_PORT% (PID %GW_PID%) but its command
echo           line could not be read: %GW_PROC%
echo           Nothing was killed. Check it yourself before stopping it:
echo             netstat -ano ^| findstr :%GW_PORT%
echo             tasklist /FI "PID eq %GW_PID%" /V
exit /b 1

:kill
echo [stop] PID %GW_PID% on port %GW_PORT%
taskkill /PID %GW_PID% /F >nul 2>&1
set "WAITED="
:verify
set "SEEN="
for /f "tokens=5" %%a in ('netstat -ano ^| findstr /r /c:":%GW_PORT% .*LISTENING"') do set "SEEN=%%a"
if not defined SEEN goto :stopped
if defined WAITED goto :stuck
set "WAITED=1"
ping -n 4 127.0.0.1 >nul
goto :verify

:stuck
echo [FAILED] PID %SEEN% still listens on port %GW_PORT% - taskkill did not take effect.
exit /b 1

:stopped
echo [OK] stopped, port %GW_PORT% is free.
exit /b 0

:not_running
echo [not running] nothing listens on port %GW_PORT%.
exit /b 0
