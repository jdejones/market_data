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

echo Running %SCRIPT_NAME%
"%PYTHON_EXE%" "%SCRIPT_PATH%"
set "EXIT_CODE=%errorlevel%"

if not "%EXIT_CODE%"=="0" (
    >> "%LOG_FILE%" echo [%date% %time%] %SCRIPT_NAME% failed with exit code %EXIT_CODE%.
    echo %SCRIPT_NAME% failed with exit code %EXIT_CODE%. See "%LOG_FILE%".
    exit /b %EXIT_CODE%
)

exit /b 0