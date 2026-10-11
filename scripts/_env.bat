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
rem calling a script: GW_JDK, GW_MVN, GW_BASE, GW_LOG_DIR, GW_DB_PATH,
rem GW_PORT, GW_HOST. Run scripts\doctor.bat to see the resolved result.
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

rem Remember what the caller set, before the defaults are filled in.
set "GW_PORT_SET=%GW_PORT%"
set "GW_HOST_SET=%GW_HOST%"

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
set "YAML_PORT="
for /f "delims=" %%p in ('powershell -NoProfile -Command "if (Test-Path -LiteralPath '%APP_YML%') { $m = Select-String -LiteralPath '%APP_YML%' -Pattern '^\s+port:\s*([0-9]+)' | Select-Object -First 1; if ($m) { $m.Matches[0].Groups[1].Value } }"') do if not defined YAML_PORT set "YAML_PORT=%%p"
if defined YAML_PORT if not defined GW_PORT set "GW_PORT=%YAML_PORT%"
if not defined GW_PORT set "GW_PORT=8317"

set "PORT_SRC=built-in default 8317"
if defined YAML_PORT set "PORT_SRC=application.yaml"
if defined GW_PORT_SET set "PORT_SRC=GW_PORT you set"

set "PORT_INJECT="
if defined GW_PORT_SET set "PORT_INJECT=-Dserver.port=%GW_PORT% -Dserver.address=%GW_HOST%"
if defined GW_HOST_SET if not defined GW_PORT_SET set "PORT_INJECT=-Dserver.address=%GW_HOST%"

rem --- java ---------------------------------------------------------------
rem Look for a JDK *directory*, in this order: GW_JDK, the project's tools\
rem folder, JAVA_HOME, then whatever java.exe is on the PATH.
set "JAVA_DIR="
set "JAVA_EXE="
if defined GW_JDK if exist "%GW_JDK%\bin\java.exe" set "JAVA_DIR=%GW_JDK%"
if not defined JAVA_DIR for /d %%d in ("%APP%\tools\jdk-*") do if not defined JAVA_DIR if exist "%%~fd\bin\java.exe" set "JAVA_DIR=%%~fd"
if not defined JAVA_DIR if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "JAVA_DIR=%JAVA_HOME%"
if not defined JAVA_DIR (
  for /f "delims=" %%p in ('where java.exe 2^>nul') do if not defined JAVA_EXE set "JAVA_EXE=%%p"
)
if not defined JAVA_DIR if defined JAVA_EXE for %%i in ("%JAVA_EXE%\..\..") do set "JAVA_DIR=%%~fi"

rem javaw.exe starts the service without a console window; java.exe is used
rem for the version probe because a windowless launcher's -version output
rem cannot be captured by a script.
set "JAVA_BIN=%JAVA_DIR%\bin\javaw.exe"
set "JAVA_PROBE=%JAVA_DIR%\bin\java.exe"
if exist "%JAVA_BIN%" goto :java_have_launcher
set "JAVA_BIN=%JAVA_DIR%\bin\java.exe"
:java_have_launcher
if exist "%JAVA_PROBE%" goto :java_have_probe
set "JAVA_PROBE=%JAVA_BIN%"
:java_have_probe
if not exist "%JAVA_BIN%" set "JAVA_BIN="
if not exist "%JAVA_PROBE%" set "JAVA_PROBE="

rem Must be 21 or newer: Spring Boot 3 needs virtual threads, and starting the
rem jar on an old java fails without a visible message.
rem This line must stay outside any ( ) block - the parenthesised PowerShell
rem command inside it would close the block early and break the parser.
set "JAVA_OK21="
set "JAVA_VERDICT="
set "JAVA_NUM="
if not defined JAVA_PROBE goto :ver_done
for /f "tokens=1,2" %%v in ('powershell -NoProfile -Command "$t = ((& '%JAVA_PROBE%' -version 2>&1 | Out-String) -replace '[\r\n\t]+',' ').Trim(); $m = [regex]::Match($t, '[0-9][0-9._]*'); if (-not $m.Success) { Write-Output 'UNKNOWN none'; exit }; $p = $m.Value.Split('.'); $j = [int]$p[0]; if ($j -eq 1 -and $p.Count -gt 1) { $j = [int]$p[1] }; if ($j -ge 21) { Write-Output ('OK ' + $m.Value) } else { Write-Output ('OLD ' + $m.Value) }"') do call :pickver %%v %%w
:ver_done
if "%JAVA_VERDICT%"=="OK" set "JAVA_OK21=1"

rem Maven compiles with javac, so it needs a JDK directory, not a JRE.
if defined JAVA_DIR if exist "%JAVA_DIR%\bin\javac.exe" set "JAVA_HOME=%JAVA_DIR%"

rem --- maven --------------------------------------------------------------
rem Priority: GW_MVN > project tools\apache-maven-*\bin\mvn.cmd > mvn on PATH.
set "MVN="
set "MVN_SETTINGS="
if defined GW_MVN if exist "%GW_MVN%" set "MVN=%GW_MVN%"
if not defined MVN for /d %%d in ("%APP%\tools\apache-maven-*") do if not defined MVN if exist "%%~fd\bin\mvn.cmd" set "MVN=%%~fd\bin\mvn.cmd"
if defined MVN if exist "%APP%\tools\settings.xml" set "MVN_SETTINGS=%APP%\tools\settings.xml"
if not defined MVN (
  for /f "delims=" %%p in ('where mvn.cmd 2^>nul') do if not defined MVN set "MVN=%%p"
)
if not defined MVN (
  for /f "delims=" %%p in ('where mvn 2^>nul') do if not defined MVN set "MVN=%%p"
)
set "MVN_EXTRA="
if defined MVN_SETTINGS set "MVN_EXTRA=-s "%MVN_SETTINGS%""

exit /b 0

rem Keeps only the first answer the probe returns.
:pickver
if defined JAVA_NUM exit /b 0
set "JAVA_VERDICT=%~1"
set "JAVA_NUM=%~2"
exit /b 0
