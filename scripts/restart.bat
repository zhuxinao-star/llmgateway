@echo off
setlocal
rem -----------------------------------------------------------------------
rem Stop, start, then show status.
rem The gateway needs a few seconds before it binds the port, and how many
rem depends on the machine, so this waits for the port to actually appear
rem instead of sleeping a guessed number of seconds. ping is the only sleep a
rem batch file here can use - powershell is refused.
rem If stop.bat refuses (another program owns the port) it stops right there:
rem starting a second gateway beside it only fails to bind and leaves you
rem guessing which of the two answered.
rem -----------------------------------------------------------------------
call "%~dp0_env.bat"
if errorlevel 1 exit /b 1

call "%~dp0stop.bat"
if errorlevel 1 goto :refused
ping -n 2 127.0.0.1 >nul
call "%~dp0start.bat"
if errorlevel 1 goto :start_failed

echo.
echo waiting for port %GW_PORT% to bind
set "TICKS=0"
:wait_bind
set "BOUND="
for /f "tokens=5" %%a in ('netstat -ano ^| findstr /r /c:":%GW_PORT% .*LISTENING"') do set "BOUND=%%a"
if defined BOUND goto :bound
set /a TICKS+=1
if %TICKS% GEQ 40 goto :too_slow
ping -n 2 127.0.0.1 >nul
goto :wait_bind

:bound
echo [up] port %GW_PORT% is listening after %TICKS%s, PID %BOUND%
echo.
call "%~dp0status.bat"
endlocal
exit /b 0

:too_slow
echo [FAILED] nothing listens on port %GW_PORT% after %TICKS%s.
echo          the JVM is probably still booting or already dead - look:
echo            scripts\status.bat log
endlocal
exit /b 1

:refused
echo.
echo [restart aborted] stop.bat would not touch the port - see its message above.
endlocal
exit /b 1

:start_failed
echo.
echo [restart aborted] start.bat did not launch the gateway.
endlocal
exit /b 1
