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
powershell -NoProfile -Command "if (Get-NetTCPConnection -LocalPort %GW_PORT% -State Listen -ErrorAction SilentlyContinue) { exit 9 }"
if errorlevel 9 goto :already_running

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

:already_running
echo [already running] http://%GW_HOST%:%GW_PORT%/ui/index.html
exit /b 0
