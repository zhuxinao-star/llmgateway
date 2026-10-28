@echo off
setlocal
rem -----------------------------------------------------------------------
rem Build the runnable fat jar: target\llmGateway.jar
rem Needs a full JDK 21+ (with javac) and Maven. Both can come from your PATH,
rem from JAVA_HOME, or from a portable copy dropped into tools\.
rem The running service locks the jar file, so it has to be stopped first.
rem -----------------------------------------------------------------------
call "%~dp0_env.bat"
if errorlevel 1 exit /b 1

if defined MVN goto :mvn_ok
echo [maven missing] install Maven 3.8+ so mvn is on your PATH,
echo                 or set GW_MVN to the full path of mvn.cmd.
echo                 scripts\doctor.bat shows what was searched.
exit /b 1

:mvn_ok
if defined JAVA_HOME goto :jdk_ok
echo [jdk missing] need a full JDK with javac.exe, not only a java runtime.
echo               java was found at: %JAVA_DIR%
echo               run scripts\doctor.bat, then set GW_JDK to a JDK 21 directory
exit /b 1

:jdk_ok
if defined JAVA_OK21 goto :version_ok
echo [jdk too old] version found: %JAVA_NUM%
echo               at: %JAVA_DIR%
echo               Spring Boot 3 needs JDK 21+ - set GW_JDK to a JDK 21
echo               directory, or drop a portable jdk-21.* folder into tools\
exit /b 1

:version_ok
rem Only our own JVM locks target\llmGateway.jar. Another program on the same
rem port does not, so the build is refused only when the listener is this
rem project's gateway - or when _gw.bat could not tell who it is.
call "%~dp0_gw.bat" %GW_PORT%
if not defined GW_PID goto :port_free
if "%GW_OWN%"=="0" goto :port_free
echo [service running] port %GW_PORT% is already held (PID %GW_PID%): %GW_PROC%
echo                   If that is llmGateway, the jar file is locked - stop it
echo                   with scripts\stop.bat and build again.
exit /b 9

:port_free
echo [jdk]   %JAVA_HOME%
echo [mvn]   %MVN%
pushd "%APP%"
call "%MVN%" %MVN_EXTRA% package -DskipTests
if not errorlevel 1 goto :build_ok
popd
echo.
echo [build failed] see the maven output above. Common causes:
echo                - dependencies cannot be downloaded: use a maven mirror
echo                - JDK below 21: Spring Boot 3 needs virtual threads
exit /b 1

:build_ok
popd
echo.
echo [OK] %APP%\target\llmGateway.jar
echo      next: scripts\start.bat
exit /b 0
