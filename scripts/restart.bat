@echo off
setlocal
rem Stop, start, then show status. Start needs a few seconds to bind the port,
rem so the status probe waits instead of racing it.
call "%~dp0stop.bat"
powershell -NoProfile -Command "Start-Sleep -Seconds 2"
call "%~dp0start.bat"
powershell -NoProfile -Command "Start-Sleep -Seconds 8"
call "%~dp0status.bat"
endlocal
