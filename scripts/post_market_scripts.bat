@echo off
REM Post-market processing is a sequential, fail-fast pipeline.
REM daily_after_close_processing.py refreshes the core daily artifacts used by later
REM steps. recent_events.py then reads those artifacts and updates recent event data.
REM The OpenClaw transfer scripts run last so they only publish/export data after the
REM upstream processing succeeds. Unlike pre_market_scripts.bat, these scripts are
REM expected to finish, so they run in the same terminal and each step waits for the
REM previous one. If any script exits nonzero, log the failure and stop the pipeline.
set "PYTHON_EXE=C:\Users\jdejo\AppData\Local\Programs\Python\Python313\python.exe"
set "LOG_FILE=C:\Users\jdejo\Market_Data_Processing\market_data\scripts\script_error_logs.txt"
set "DETAILED_LOG_FILE=C:\Users\jdejo\Market_Data_Processing\market_data\scripts\scripts_error_logs_detailed.txt"

cd /d "C:\Users\jdejo\Market_Data_Processing\market_data"

call :run_script "daily_after_close_processing.py" "C:\Users\jdejo\Market_Data_Processing\market_data\scripts\daily_after_close_processing.py"
if errorlevel 1 exit /b %errorlevel%

call :run_script "recent_events.py" "C:\Users\jdejo\Market_Data_Processing\market_data\scripts\recent_events.py"
if errorlevel 1 exit /b %errorlevel%

call :run_script "pkl_to_openclaw.py" "C:\Users\jdejo\Market_Data_Processing\market_data\scripts\pkl_to_openclaw.py"
if errorlevel 1 exit /b %errorlevel%

call :run_script "db_to_openclaw.py" "C:\Users\jdejo\Market_Data_Processing\market_data\scripts\db_to_openclaw.py"
if errorlevel 1 exit /b %errorlevel%

exit /b 0

:run_script
set "SCRIPT_NAME=%~1"
set "SCRIPT_PATH=%~2"
set "SCRIPT_STDERR=%TEMP%\%~n1_stderr_%RANDOM%%RANDOM%.log"

echo Running %SCRIPT_NAME%
"%PYTHON_EXE%" "%SCRIPT_PATH%" 2> "%SCRIPT_STDERR%"
set "EXIT_CODE=%errorlevel%"

if not "%EXIT_CODE%"=="0" (
    type nul > "%DETAILED_LOG_FILE%"
    >> "%DETAILED_LOG_FILE%" echo [%date% %time%] %SCRIPT_NAME% failed with exit code %EXIT_CODE%.
    >> "%DETAILED_LOG_FILE%" echo.
    if exist "%SCRIPT_STDERR%" type "%SCRIPT_STDERR%" >> "%DETAILED_LOG_FILE%"
    >> "%LOG_FILE%" echo [%date% %time%] %SCRIPT_NAME% failed with exit code %EXIT_CODE%.
    echo %SCRIPT_NAME% failed with exit code %EXIT_CODE%. See "%LOG_FILE%".
    if exist "%SCRIPT_STDERR%" del "%SCRIPT_STDERR%" > nul 2>&1
    exit /b %EXIT_CODE%
)

if exist "%SCRIPT_STDERR%" del "%SCRIPT_STDERR%" > nul 2>&1
exit /b 0