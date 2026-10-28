@echo off
setlocal
rem -----------------------------------------------------------------------
rem Start llmGateway.
rem The working directory is forced to the project folder because the app
rem reads its config from the relative path config\config.yaml.
rem No path is hardcoded here: scripts\_env.bat resolves everything and
rem honours GW_JDK / GW_MVN / GW_BASE / GW_LOG_DIR / GW_DB_PATH / GW_PORT /
rem GW_HOST. Run scripts\doctor.bat first to see what the scripts found.
rem -----------------------------------------------------------------------
call "%~dp0_env.bat"
if errorlevel 1 exit /b 1

rem Error branches are kept flat (no parentheses blocks) because a path can
rem contain brackets and parentheses, e.g. C:\Program Files (x86)\...
if defined JAVA_BIN goto :have_java
echo [java missing] no JDK found. Put a portable jdk-21.* folder in tools\,
echo                or set JAVA_HOME, or set GW_JDK to a JDK 21 directory.
echo                scripts\doctor.bat shows exactly what was searched.
exit /b 1

:have_java
if defined JAVA_OK21 goto :have_java21
echo [java too old] the gateway needs JDK 21 or newer.
echo                version found : %JAVA_NUM%
echo                located at    : %JAVA_DIR%
echo                set GW_JDK to a JDK 21 directory or drop one into tools\
exit /b 1

:have_java21
if exist "%CFG%" goto :have_config
echo [config missing] copy %APP%\config\config.example.yaml
echo                  to %CFG% and fill in your own provider keys
exit /b 1

:have_config
if exist "%JAR%" goto :have_jar
echo [jar missing] run scripts\build.bat first
exit /b 1

:have_jar
rem Is the port already taken, and if so by whom? _gw.bat reads the listener's command
rem line, because another java program on this machine uses the same port and starting
rem a second gateway next to it would only fail to bind.
call "%~dp0_gw.bat" %GW_PORT%
if not defined GW_PID goto :launch
if not "%GW_OWN%"=="1" goto :port_taken

echo [already running] http://%GW_HOST%:%GW_PORT%/ui/index.html
echo                   PID %GW_PID% - stop it first with scripts\stop.bat
exit /b 0

:port_taken
echo [port busy] port %GW_PORT% is held by another program, not llmGateway:
echo             %GW_PROC%
echo             Nothing was started. Either stop that program, or give the gateway
echo             its own port: set GW_PORT=9000 (scripts\doctor.bat shows the effect).
exit /b 1

:launch
rem Create the folders the app writes into.
if not exist "%GW_LOG_DIR%" mkdir "%GW_LOG_DIR%"
for %%i in ("%GW_DB_PATH%") do set "DB_DIR=%%~dpi"
if not exist "%DB_DIR%" mkdir "%DB_DIR%"

start "" /D "%APP%" "%JAVA_BIN%" -Dfile.encoding=UTF-8 %PORT_INJECT% -jar "%JAR%"
echo [starting] http://%GW_HOST%:%GW_PORT%/ui/index.html
echo [java]     %JAVA_DIR%
echo [logs]     %GW_LOG_DIR%\app.log
echo [db]       %GW_DB_PATH%
echo [port]     %GW_PORT% from %PORT_SRC%
exit /b 0
