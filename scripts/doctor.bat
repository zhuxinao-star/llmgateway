@echo off
setlocal
rem -----------------------------------------------------------------------
rem Self-check: print every path and tool this folder's scripts resolved and
rem say what is missing. Run this first after downloading the repo.
rem Written flat with labels, not with ( ) blocks, because the paths and the
rem tool versions echoed here can contain brackets and parentheses.
rem -----------------------------------------------------------------------
call "%~dp0_env.bat"
if not errorlevel 1 goto :layout_ok
echo.
echo To run the project somewhere else, copy or move the whole project folder -
echo scripts\ alone cannot find it.
exit /b 1

:layout_ok
echo ================ llmGateway path check ================
echo project root    : %APP%
echo config file path: %CFG%
echo jar path        : %JAR%
echo listening at    : http://%GW_HOST%:%GW_PORT%/ui/index.html
echo log directory   : %GW_LOG_DIR%
echo sqlite database : %GW_DB_PATH%
echo.

if exist "%CFG%" goto :cfg_ok
echo config          : MISSING
echo                 copy config\config.example.yaml to config\config.yaml
echo                 and fill in your own provider keys before starting
goto :cfg_done

:cfg_ok
echo config          : found

:cfg_done
if exist "%JAR%" goto :jar_ok
echo build output    : MISSING - run scripts\build.bat
echo                 it produces target\llmGateway.jar
goto :jar_done

:jar_ok
echo build output    : found

:jar_done
if defined JAVA_DIR goto :java_ok
echo java            : NOT FOUND
echo                 install JDK 21+, or drop a portable jdk-21.* folder into
echo                 tools\, or set GW_JDK to the JDK directory
goto :java_done

:java_ok
echo java            : %JAVA_DIR%
if not defined JAVA_NTRIED goto :java_version
echo                 : %JAVA_NTRIED% java candidates searched, first JDK 21+ one wins
:java_version
if not defined JAVA_NUM goto :java_done
if not "%JAVA_VERDICT%"=="OK" goto :java_old
echo version         : %JAVA_NUM% - OK, 21 or newer
goto :java_done

:java_old
echo version         : %JAVA_NUM% - TOO OLD, the gateway needs 21 or newer
echo                 set GW_JDK to a JDK 21 directory, or put a portable
echo                 jdk-21.* folder into tools\ and run doctor again

:java_done
if exist "%JAVA_DIR%\bin\javac.exe" goto :javac_ok
echo compiler        : no javac here, build.bat cannot compile - a JRE is not
echo                 enough, point GW_JDK at a full JDK
goto :javac_done

:javac_ok
echo compiler        : javac present, build.bat can compile

:javac_done
if defined MVN goto :mvn_ok
echo maven           : NOT FOUND - only needed to build. Install Maven, or set
echo                 GW_MVN to the full path of mvn.cmd
goto :mvn_done

:mvn_ok
echo maven           : %MVN%

:mvn_done
echo port            : %GW_PORT% from %PORT_SRC%
if defined PORT_INJECT goto :port_inject
echo                 : the app binds the port from src\main\resources\application.yaml
goto :port_done

:port_inject
echo                 : GW_PORT is injected into the JVM, so the app binds it

:port_done
rem Who actually holds the port right now, and is it ours? Same helper the
rem start/stop/build scripts use, so doctor reports exactly what they will act on.
call "%~dp0_gw.bat" %GW_PORT%
if not defined GW_PID goto :port_free
if "%GW_OWN%"=="1" goto :port_ours
if "%GW_OWN%"=="0" goto :port_other
echo port state      : BUSY - PID %GW_PID%, identity unclear: %GW_PROC%
echo                 : stop.bat will refuse to kill an unidentified process.
goto :port_footer

:port_ours
echo port state      : BUSY - llmGateway is listening, PID %GW_PID%
goto :port_footer

:port_other
echo port state      : BUSY - ANOTHER PROGRAM, not llmGateway, holds PID %GW_PID%
echo                 : %GW_PROC%
echo                 : the gateway cannot bind this port while that runs. Either stop
echo                 : it, or set GW_PORT to a port of its own.
goto :port_footer

:port_free
echo port state      : free, nothing listens on %GW_PORT%

:port_footer
echo.
echo To change anything, set an environment variable before running a script:
echo   set GW_JDK=D:\java\jdk-21
echo   set GW_MVN=C:\maven\bin\mvn.cmd
echo   set GW_MVN_SETTINGS=C:\maven\conf\settings.xml
echo   set GW_BASE=D:\llmgateway-data
echo   set GW_PORT=9000
echo   set GW_HOST=0.0.0.0
echo   set GW_DB_PATH=D:\mydata\llm-gateway.db
echo   set GW_LOG_DIR=D:\mylogs
echo GW_BASE moves data\ and logs\ to one place; GW_DB_PATH and GW_LOG_DIR win
echo over it. GW_PORT and GW_HOST are injected into the JVM so they really
echo change what the app binds - the server section of config\config.yaml
echo only feeds the startup log line. Run doctor again to confirm.
exit /b 0
