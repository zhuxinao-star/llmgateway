@echo off
rem -----------------------------------------------------------------------
rem Shared path resolver for every script in this folder.
rem Called by start/stop/restart/status/build/doctor - do not run it alone.
rem
rem It answers five questions once, so no other script hardcodes a location:
rem   1. APP     - where the project lives (the folder above this one)
rem   2. JAVA_*  - which JDK runs the jar and compiles the source
rem   3. MVN     - which Maven builds
rem   4. PORT    - what the service listens on
rem   5. LOG_DIR / DB_PATH - where the service writes
rem
rem Every value can be overridden by an environment variable you set BEFORE
rem calling a script: GW_JDK, GW_MVN, GW_MVN_SETTINGS, GW_BASE, GW_LOG_DIR,
rem GW_DB_PATH, GW_PORT, GW_HOST. Run scripts\doctor.bat to see the result.
rem
rem Nothing in this file uses PowerShell: on the machine this project grew on,
rem a batch file is refused permission to start powershell.exe at all, so every
rem probe that used to call it silently answered "nothing found". Use netstat,
rem findstr, curl and cscript instead.
rem -----------------------------------------------------------------------

set "SCRIPT_DIR=%~dp0"
for %%i in ("%SCRIPT_DIR%..") do set "APP=%%~fi"

rem The project root must contain pom.xml. If it does not, this script copy
rem was put in the wrong place - fail loudly instead of starting nothing.
if not exist "%APP%\pom.xml" (
  echo [layout error] pom.xml not found in %APP%
  echo                the scripts\ folder must stay inside the project root
  exit /b 1
)

rem --- data and logs: default inside the project folder -------------------
rem Nothing is ever searched in parent folders on purpose: a sibling folder
rem that happens to be named data\ or logs\ would silently steal the paths.
if not defined GW_BASE    set "GW_BASE=%APP%"
if not defined GW_LOG_DIR set "GW_LOG_DIR=%GW_BASE%\logs"
if not defined GW_DB_PATH set "GW_DB_PATH=%GW_BASE%\data\llm-gateway.db"
if not defined GW_HOST    set "GW_HOST=127.0.0.1"

set "JAR=%APP%\target\llmGateway.jar"
set "CFG=%APP%\config\config.yaml"
set "APP_YML=%APP%\src\main\resources\application.yaml"

rem --- port ---------------------------------------------------------------
rem The app takes its port from application.yaml. The scripts have to agree
rem with it, otherwise they probe a port the service never binds. If you set
rem GW_PORT it wins AND is injected into the JVM, so the two cannot disagree.
rem The first indented "port: <number>" line in that file is the server port.
set "YAML_PORT="
if not exist "%APP_YML%" goto :port_no_yaml
for /f "eol=# tokens=2 delims=:" %%p in ('findstr /r /c:"^  *port: *[0-9]" "%APP_YML%"') do call :portline %%p
:port_no_yaml
rem The port the app will bind by itself: application.yaml, else Spring's own 8317.
set "APP_PORT=%YAML_PORT%"
if not defined APP_PORT set "APP_PORT=8317"

set "PORT_INJECT="
if not defined GW_PORT goto :port_from_app
rem The caller named a port that application.yaml does not already bind: it has to be
rem injected into the JVM, otherwise the app keeps its own port and the scripts probe a
rem port nothing listens on. Naming the same number the yaml already says is not an
rem override, so it is reported as what the app would do anyway.
if "%GW_PORT%"=="%APP_PORT%" goto :port_from_app
set "PORT_SRC=GW_PORT you set"
set "PORT_INJECT=-Dserver.port=%GW_PORT%"
goto :port_host

:port_from_app
set "GW_PORT=%APP_PORT%"
set "PORT_SRC=application.yaml"
if defined YAML_PORT goto :port_host
set "PORT_SRC=built-in default 8317, application.yaml has no port line"

:port_host
rem 127.0.0.1 is what the app binds when told nothing, so only a different host is worth
rem injecting. Decided by value, not by "was the variable set", so calling this file
rem twice in the same shell cannot turn a default into a caller override.
if not defined GW_HOST goto :port_after
if "%GW_HOST%"=="127.0.0.1" goto :port_after
if defined PORT_INJECT set "PORT_INJECT=%PORT_INJECT% -Dserver.address=%GW_HOST%"
if not defined PORT_INJECT set "PORT_INJECT=-Dserver.address=%GW_HOST%"

:port_after

rem --- java ---------------------------------------------------------------
rem Every candidate is asked for its real version and the FIRST one that is 21
rem or newer wins. Candidates, in order: GW_JDK, the project's tools\ folder,
rem JAVA_HOME, then java.exe on the PATH.
rem
rem Probing instead of picking the first directory that exists matters: this
rem machine's JAVA_HOME still points at JDK 8 while a JDK 21 sits in tools\, and
rem the old resolver stopped at JAVA_HOME, so start.bat refused every launch
rem with [java too old] even though a usable JDK was one folder away.
set "JAVA_DIR="
set "JAVA_OK21="
set "JAVA_VERDICT="
set "JAVA_MAJOR="
set "JAVA_NUM="
set "JAVA_FIRST="
set "JAVA_OLD="
set "JAVA_REMEMBERED="
set "JAVA_NTRIED=0"

call :tryjava "%GW_JDK%"
for /d %%d in ("%APP%\tools\jdk-*") do call :tryjava "%%~fd"
call :tryjava "%JAVA_HOME%"
for /f "delims=" %%p in ('where java.exe 2^>nul') do call :tryjavapath "%%p"

if defined JAVA_OK21 goto :java_selected
rem Nothing usable: report the oldest-searched candidate so the message names a
rem directory the user can actually act on.
set "JAVA_DIR=%JAVA_OLD%"
if not defined JAVA_DIR set "JAVA_DIR=%JAVA_FIRST%"

:java_selected
set "JAVA_BIN="
set "JAVA_PROBE="
if not defined JAVA_DIR goto :java_paths_done
rem javaw.exe starts the service without a console window; java.exe is used for
rem the version probe because a windowless launcher's -version output cannot be
rem captured by a script.
set "JAVA_PROBE=%JAVA_DIR%\bin\java.exe"
set "JAVA_BIN=%JAVA_DIR%\bin\javaw.exe"
if exist "%JAVA_BIN%" goto :java_paths_done
set "JAVA_BIN=%JAVA_DIR%\bin\java.exe"
if exist "%JAVA_BIN%" goto :java_paths_done
set "JAVA_BIN="

:java_paths_done
rem Maven compiles with javac, so it needs a JDK directory, not a JRE.
if defined JAVA_DIR if exist "%JAVA_DIR%\bin\javac.exe" set "JAVA_HOME=%JAVA_DIR%"

rem --- maven --------------------------------------------------------------
rem Priority: GW_MVN > project tools\apache-maven-*\bin\mvn.cmd > mvn on PATH.
set "MVN="
set "MVN_SETTINGS="
if defined GW_MVN if exist "%GW_MVN%" set "MVN=%GW_MVN%"
if not defined MVN for /d %%d in ("%APP%\tools\apache-maven-*") do if not defined MVN if exist "%%~fd\bin\mvn.cmd" set "MVN=%%~fd\bin\mvn.cmd"
if defined GW_MVN_SETTINGS if exist "%GW_MVN_SETTINGS%" set "MVN_SETTINGS=%GW_MVN_SETTINGS%"
if not defined MVN_SETTINGS if exist "%APP%\tools\settings.xml" set "MVN_SETTINGS=%APP%\tools\settings.xml"
if not defined MVN (
  for /f "delims=" %%p in ('where mvn.cmd 2^>nul') do if not defined MVN set "MVN=%%p"
)
if not defined MVN (
  for /f "delims=" %%p in ('where mvn 2^>nul') do if not defined MVN set "MVN=%%p"
)
set "MVN_EXTRA="
if defined MVN_SETTINGS set "MVN_EXTRA=-s "%MVN_SETTINGS%""

exit /b 0

rem Keeps only the first answer the port probe returns.
:portline
if defined YAML_PORT exit /b 0
set "YAML_PORT=%~1"
exit /b 0

rem -----------------------------------------------------------------------
rem :tryjava <jdk directory> - probe one candidate and keep it when it works.
rem :CUR_CAND is the directory being probed; the version answers come from
rem _ver.js because java prints its version to stderr and a FOR /F command with
rem a path containing a space is a quoting trap cmd does not survive.
rem -----------------------------------------------------------------------
:tryjava
set "CUR_CAND=%~1"
if not defined CUR_CAND exit /b 0
if not exist "%CUR_CAND%\bin\java.exe" exit /b 0
if defined JAVA_OK21 exit /b 0
if defined JAVA_FIRST goto :tryjava_probe
set "JAVA_FIRST=%CUR_CAND%"
:tryjava_probe
set /a JAVA_NTRIED+=1
set "JVER="
for /f "tokens=1,2,*" %%v in ('cscript //nologo "%SCRIPT_DIR%_ver.js" "%CUR_CAND%\bin\java.exe"') do call :pickver %%v %%w "%%x"
if defined JAVA_OK21 exit /b 0
goto :tryjava_remember

rem Nothing accepted from this candidate: keep the first bad answer, because that is
rem the directory the final message should name (it was searched first).
:tryjava_remember
if defined JAVA_REMEMBERED exit /b 0
set "JAVA_REMEMBERED=1"
set "JAVA_OLD=%CUR_CAND%"
set "JAVA_VERDICT=%VD%"
set "JAVA_MAJOR=%VMJ%"
set "JAVA_NUM=%VNUM%"
exit /b 0

:pickver
set "VD=%~1"
set "VMJ=%~2"
set "VNUM=%~3"
if defined JAVA_OK21 exit /b 0
if /i not "%VD%"=="OK" exit /b 0
set "JAVA_OK21=1"
set "JAVA_VERDICT=OK"
set "JAVA_MAJOR=%VMJ%"
set "JAVA_NUM=%VNUM%"
set "JAVA_DIR=%CUR_CAND%"
exit /b 0

rem :tryjavapath <full path of a java.exe> - turn it into its JDK directory.
:tryjavapath
if defined JAVA_OK21 exit /b 0
for %%i in ("%~1\..\..") do call :tryjava "%%~fi"
exit /b 0
