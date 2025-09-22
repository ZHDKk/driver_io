@echo off
chcp 1252 > nul
setlocal enabledelayedexpansion

:: Check for administrator privileges
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo Requesting administrator privileges...
    powershell -Command "Start-Process cmd -ArgumentList '/c %0' -Verb RunAs"
    exit /b
)

echo ==========================================
echo    Windows NTP Server Auto-Configuration
echo ==========================================
echo.

:: Set NTP server configuration
set NTP_SERVER=pool.ntp.org
set TYPE=NTP
set NTPSERVER_ENABLED=1

:: Stop Windows Time service
echo [1/7] Stopping Windows Time service...
net stop w32time >nul 2>&1

:: Configure time service as NTP server mode
echo [2/7] Configuring time service as NTP server...
w32tm /unregister >nul 2>&1
w32tm /register >nul 2>&1

:: Set NTP server configuration
echo [3/7] Setting NTP server configuration...
reg add "HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\W32Time\Parameters" /v "NtpServer" /d "%NTP_SERVER%,0x9" /t REG_SZ /f >nul
reg add "HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\W32Time\Parameters" /v "Type" /d "%TYPE%" /t REG_SZ /f >nul

:: Enable NTP server
echo [4/7] Enabling NTP server functionality...
reg add "HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\W32Time\TimeProviders\NtpServer" /v "Enabled" /t REG_DWORD /d %NTPSERVER_ENABLED% /f >nul

:: Configure time service parameters
echo [5/7] Configuring advanced time service parameters...
reg add "HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\W32Time\Config" /v "AnnounceFlags" /t REG_DWORD /d 5 /f >nul

:: Configure firewall to allow NTP traffic
echo [6/7] Configuring firewall to allow NTP traffic (port 123/UDP)...
netsh advfirewall firewall add rule name="NTP Server (UDP 123)" dir=in action=allow protocol=UDP localport=123 >nul 2>&1

:: Start Windows Time service
echo [7/7] Starting Windows Time service...
net start w32time >nul 2>&1

:: Force time service to reload configuration
w32tm /config /update >nul

echo.
echo ==========================================
echo    NTP Server Configuration Complete!
echo ==========================================
echo.
echo Configuration Summary:
echo   - NTP Server: %NTP_SERVER%
echo   - Server Type: %TYPE%
echo   - NTP Service: Enabled
echo   - Firewall Rule: Added (UDP port 123)
echo.
echo Verification Commands:
echo   - Check time service status: w32tm /query /status
echo   - Test NTP service: w32tm /stripchart /computer:127.0.0.1
echo.
pause