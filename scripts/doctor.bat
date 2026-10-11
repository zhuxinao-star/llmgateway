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
powershell -NoProfile -Command "if (Get-NetTCPConnection -LocalPort %GW_PORT% -State Listen -ErrorAction SilentlyContinue) { Write-Output 'port state      : BUSY, a gateway is already listening' } else { Write-Output 'port state      : free' }"
echo.
echo To change anything, set an environment variable before running a script:
echo   set GW_JDK=D:\java\jdk-21
echo   set GW_MVN=C:\maven\bin\mvn.cmd
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
