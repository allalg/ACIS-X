@echo off
REM ========================================================================
REM ACIS-X Complete Cleanup & Fresh Start
REM ========================================================================
REM
REM This script:
REM 1. Kills all Python processes
REM 2. Waits for file handles to release
REM 3. Deletes corrupted database and logs
REM 4. Starts a fresh ACIS-X instance
REM
REM To stop the system: Press Ctrl+C in this window
REM
REM ========================================================================

setlocal enabledelayedexpansion

echo.
echo ========================================================================
echo ACIS-X SYSTEM CLEANUP & RESTART
echo ========================================================================
echo.

echo [Step 1/4] Stopping all running processes...
echo Please wait...

for /f "tokens=2" %%A in ('tasklist ^| find /i "python"') do (
    taskkill /PID %%A /F >nul 2>&1
)

timeout /t 5 /nobreak

echo [Step 2/4] Waiting for file locks to release...
set retry=0
:wait_loop
if %retry% gtr 20 goto cleanup_files
dir acis.db >nul 2>&1
if errorlevel 1 goto cleanup_files
timeout /t 1 /nobreak
set /a retry=%retry%+1
goto wait_loop

:cleanup_files
echo [Step 3/4] Removing old database and log files...
for %%F in (acis.db acis.db-wal acis.db-shm acis.log acis_*.log acis.pid) do (
    if exist %%F (
        del /Q %%F >nul 2>&1
    )
)

echo [Step 4/4] Starting fresh ACIS-X instance...
echo.
echo ========================================================================
echo ACIS-X IS RUNNING
echo ========================================================================
echo.
echo Status: STARTING UP
echo   - Initializing Kafka topics...
echo   - Creating database schema...
echo   - Starting agents...
echo.
echo Output below. To STOP: Press Ctrl+C below
echo.
echo ========================================================================
echo.

REM Start the system with venv python
.venv\Scripts\python.exe run_acis.py

REM If we get here, system was stopped
echo.
echo ========================================================================
echo ACIS-X has stopped
echo ========================================================================
echo Check acis.log for details

pause
