@echo off
setlocal
rem -----------------------------------------------------------------------
rem Is the gateway up? Two separate questions, because on this machine another
rem program can hold the same port and answer HTTP just as happily:
rem   1. who owns the port, and is it llmGateway?   (_gw.bat)
rem   2. does something answer /api/overview?      (curl)
rem "status.bat log" also tails the log file.
rem No PowerShell anywhere: curl answers the HTTP probe - there is no
rem /api/health endpoint and /api/overview is a GET, so the admin filter never
rem blocks it - and _tail.js reads the log.
rem -----------------------------------------------------------------------
call "%~dp0_env.bat"
if errorlevel 1 exit /b 1

echo port %GW_PORT% from %PORT_SRC%
call "%~dp0_gw.bat" %GW_PORT%
if not defined GW_PID goto :free_port
if "%GW_OWN%"=="1" goto :our_port
if "%GW_OWN%"=="0" goto :other_port
echo listener        : PID %GW_PID%, identity unclear - %GW_PROC%
echo                 : check it yourself with: tasklist /FI "PID eq %GW_PID%" /V
goto :http

:our_port
echo listener        : llmGateway, PID %GW_PID%
goto :http

:other_port
echo listener        : ANOTHER program, not llmGateway
echo                 : %GW_PROC%
echo                 : stop.bat will refuse to kill it. Either stop that program
echo                 : with its own script, or give the gateway its own port:
echo                 : set GW_PORT=9000
goto :http

:free_port
echo listener        : none, the port is free

:http
echo.
echo probe http://%GW_HOST%:%GW_PORT%/api/overview
set "HTTP_CODE="
for /f "delims=" %%c in ('curl -s -m 5 -o nul -w "%%{http_code}" http://%GW_HOST%:%GW_PORT%/api/overview') do set "HTTP_CODE=%%c"
if defined HTTP_CODE goto :code_known
echo   UNKNOWN: curl is not on PATH, so nothing answered and this script cannot
echo            tell you why. The listener line above is still accurate.
goto :after_http

:code_known
if not "%HTTP_CODE%"=="000" goto :http_up
echo   DOWN: nothing answered on port %GW_PORT%
if "%GW_OWN%"=="1" echo         : the gateway owns the port but does not reply - look at the log: scripts\status.bat log
goto :after_http

:http_up
echo   UP   HTTP %HTTP_CODE%
if not "%HTTP_CODE%"=="200" echo        : a reply that is not 200 is not the gateway admin page
if "%GW_OWN%"=="1" goto :after_http
rem The port is ours to look at but not ours to kill. Before saying "that is not
rem llmGateway", check whether it is in fact the gateway API running inside some other
rem package (a single-process app that embeds the gateway answers /api/overview with its
rem own payload) - then the thing to warn about is the CODE VERSION, not the identity.
set "PROBE=%TEMP%\gw-overview-probe.txt"
curl -s -m 5 -o "%PROBE%" http://%GW_HOST%:%GW_PORT%/api/overview
findstr /c:"catalogSize" "%PROBE%" >nul 2>&1
if errorlevel 1 goto :not_gateway_api
del "%PROBE%" >nul 2>&1
echo        : that IS the gateway API, but served by the process above, not by
echo          %JAR%. Code changed with scripts\build.bat or jar-patch.bat does NOT
echo          reach that process - whatever packaged it must be rebuilt to pick up
echo          the new classes.
goto :after_http

:not_gateway_api
del "%PROBE%" >nul 2>&1
echo        : careful - whatever answered is not llmGateway, see the listener above
goto :after_http

:after_http
if /i not "%~1"=="log" goto :no_log
echo.
echo last 20 lines of %GW_LOG_DIR%\app.log
cscript //nologo "%~dp0_tail.js" "%GW_LOG_DIR%\app.log" 20
exit /b 0

:no_log
echo.
echo show recent log lines: scripts\status.bat log
exit /b 0
